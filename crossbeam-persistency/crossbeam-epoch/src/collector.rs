/// Epoch-based garbage collector.
///
/// # Examples
///
/// ```
/// use crossbeam_epoch::Collector;
///
/// let collector = Collector::new();
///
/// let handle = collector.register(None);
/// drop(collector); // `handle` still works after dropping `collector`
///
/// handle.pin().flush();
/// ```
use core::fmt;

use crate::guard::Guard;
use crate::internal::{Global, Local};
use crate::primitive::sync::Arc;

/// An epoch-based garbage collector.
pub struct Collector {
    pub(crate) global: Arc<Global>,
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl Default for Collector {
    fn default() -> Self {
        Self {
            global: Arc::new(Global::new()),
        }
    }
}

impl Collector {
    /// Creates a new collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new handle for the collector.
    pub fn register(&self, tid: Option<usize>) -> LocalHandle {
        Local::register(self, tid)
    }

    pub(crate) fn find(&self, tid: usize) -> Option<LocalHandle> {
        Local::find(self, tid)
    }
}

impl Clone for Collector {
    /// Creates another reference to the same garbage collector.
    fn clone(&self) -> Self {
        Collector {
            global: self.global.clone(),
        }
    }
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Collector { .. }")
    }
}

impl PartialEq for Collector {
    /// Checks if both handles point to the same collector.
    fn eq(&self, rhs: &Collector) -> bool {
        Arc::ptr_eq(&self.global, &rhs.global)
    }
}
impl Eq for Collector {}

/// A handle to a garbage collector.
pub struct LocalHandle {
    pub(crate) local: *const Local,
}

impl LocalHandle {
    /// Pins the handle.
    #[inline]
    pub fn pin(&self) -> Guard {
        unsafe { (*self.local).pin() }
    }

    /// Returns `true` if the handle is pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        unsafe { (*self.local).is_pinned() }
    }

    /// Returns the `Collector` associated with this handle.
    #[inline]
    pub fn collector(&self) -> &Collector {
        unsafe { (*self.local).collector() }
    }

    /// is_repinning
    #[inline]
    pub(crate) fn is_repinning(&self) -> bool {
        unsafe { (*self.local).is_repinning.get() }
    }

    #[inline]
    pub(crate) unsafe fn reset_count(&self) {
        (*self.local).reset_count()
    }

    #[inline]
    pub(crate) unsafe fn set_guard_count(&self, cnt: usize) {
        (*self.local).set_guard_count(cnt)
    }
}

impl Drop for LocalHandle {
    #[inline]
    fn drop(&mut self) {
        // @old_guard:
        // - It doesn't matter if the local handle is dropped in a thread-local panic. Only the guard should not be dropped.
        // - The purpose of local handle drop is to remove local from the global list when there are no remaining handles and guards.
        unsafe {
            Local::release_handle(&*self.local);
        }
    }
}

impl fmt::Debug for LocalHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("LocalHandle { .. }")
    }
}

#[cfg(all(test, not(crossbeam_loom)))]
mod tests {
    use std::mem;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crossbeam_utils::thread;

    use crate::{Collector, Owned};

    const NUM_THREADS: usize = 8;

    #[test]
    fn pin_reentrant() {
        let collector = Collector::new();
        let handle = collector.register(None);
        drop(collector);

        assert!(!handle.is_pinned());
        {
            let _guard = &handle.pin();
            assert!(handle.is_pinned());
            {
                let _guard = &handle.pin();
                assert!(handle.is_pinned());
            }
            assert!(handle.is_pinned());
        }
        assert!(!handle.is_pinned());
    }

    #[test]
    fn flush_local_bag() {
        let collector = Collector::new();
        let handle = collector.register(None);
        drop(collector);

        for _ in 0..100 {
            let guard = &handle.pin();
            unsafe {
                let a = Owned::new(7).into_shared(guard);
                guard.defer_destroy(a);

                assert!(!(*guard.local).bag.with(|b| (*b).is_empty()));

                while !(*guard.local).bag.with(|b| (*b).is_empty()) {
                    guard.flush();
                }
            }
        }
    }

    #[test]
    fn garbage_buffering() {
        let collector = Collector::new();
        let handle = collector.register(None);
        drop(collector);

        let guard = &handle.pin();
        unsafe {
            for _ in 0..10 {
                let a = Owned::new(7).into_shared(guard);
                guard.defer_destroy(a);
            }
            assert!(!(*guard.local).bag.with(|b| (*b).is_empty()));
        }
    }

    #[test]
    fn pin_holds_advance() {
        let collector = Collector::new();

        thread::scope(|scope| {
            for _ in 0..NUM_THREADS {
                scope.spawn(|_| {
                    let handle = collector.register(None);
                    for _ in 0..500_000 {
                        let guard = &handle.pin();

                        let before = collector.global.epoch.load(Ordering::Relaxed);
                        collector.global.collect(guard);
                        let after = collector.global.epoch.load(Ordering::Relaxed);

                        assert!(after.wrapping_sub(before) <= 2);
                    }
                });
            }
        })
        .unwrap();
    }

    #[cfg(not(crossbeam_sanitize))] // TODO: assertions failed due to `cfg(crossbeam_sanitize)` reduce `internal::MAX_OBJECTS`
    #[test]
    fn incremental() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = AtomicUsize::new(0);

        let collector = Collector::new();
        let handle = collector.register(None);

        unsafe {
            let guard = &handle.pin();
            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_shared(guard);
                guard.defer_unchecked(
                    move || {
                        drop(a.into_owned());
                        DESTROYS.fetch_add(1, Ordering::Relaxed);
                    },
                    None,
                );
            }
            guard.flush();
        }

        let mut last = 0;

        while last < COUNT {
            let curr = DESTROYS.load(Ordering::Relaxed);
            assert!(curr - last <= 1024);
            last = curr;

            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert!(DESTROYS.load(Ordering::Relaxed) == 100_000);
    }

    #[test]
    fn buffering() {
        const COUNT: usize = 10;
        static DESTROYS: AtomicUsize = AtomicUsize::new(0);

        let collector = Collector::new();
        let handle = collector.register(None);

        unsafe {
            let guard = &handle.pin();
            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_shared(guard);
                guard.defer_unchecked(
                    move || {
                        drop(a.into_owned());
                        DESTROYS.fetch_add(1, Ordering::Relaxed);
                    },
                    None,
                );
            }
        }

        for _ in 0..100_000 {
            collector.global.collect(&handle.pin());
        }
        assert!(DESTROYS.load(Ordering::Relaxed) < COUNT);

        handle.pin().flush();

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn count_drops() {
        const COUNT: usize = 100_000;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();
        let handle = collector.register(None);

        unsafe {
            let guard = &handle.pin();

            for _ in 0..COUNT {
                let a = Owned::new(Elem(7i32)).into_shared(guard);
                guard.defer_destroy(a);
            }
            guard.flush();
        }

        while DROPS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn count_destroy() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = AtomicUsize::new(0);

        let collector = Collector::new();
        let handle = collector.register(None);

        unsafe {
            let guard = &handle.pin();

            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_shared(guard);
                guard.defer_unchecked(
                    move || {
                        drop(a.into_owned());
                        DESTROYS.fetch_add(1, Ordering::Relaxed);
                    },
                    None,
                );
            }
            guard.flush();
        }

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn drop_array() {
        const COUNT: usize = 700;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();
        let handle = collector.register(None);

        let mut guard = handle.pin();

        let mut v = Vec::with_capacity(COUNT);
        for i in 0..COUNT {
            v.push(Elem(i as i32));
        }

        {
            let a = Owned::new(v).into_shared(&guard);
            unsafe {
                guard.defer_destroy(a);
            }
            guard.flush();
        }

        while DROPS.load(Ordering::Relaxed) < COUNT {
            guard.repin();
            collector.global.collect(&guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn destroy_array() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = AtomicUsize::new(0);

        let collector = Collector::new();
        let handle = collector.register(None);

        unsafe {
            let guard = &handle.pin();

            let mut v = Vec::with_capacity(COUNT);
            for i in 0..COUNT {
                v.push(i as i32);
            }

            let ptr = v.as_mut_ptr() as usize;
            let len = v.len();
            guard.defer_unchecked(
                move || {
                    drop(Vec::from_raw_parts(ptr as *const i32 as *mut i32, len, len));
                    DESTROYS.fetch_add(len, Ordering::Relaxed);
                },
                None,
            );
            guard.flush();

            mem::forget(v);
        }

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn stress() {
        const THREADS: usize = 8;
        const COUNT: usize = 100_000;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();

        thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|_| {
                    let handle = collector.register(None);
                    for _ in 0..COUNT {
                        let guard = &handle.pin();
                        unsafe {
                            let a = Owned::new(Elem(7i32)).into_shared(guard);
                            guard.defer_destroy(a);
                        }
                    }
                });
            }
        })
        .unwrap();

        let handle = collector.register(None);
        while DROPS.load(Ordering::Relaxed) < COUNT * THREADS {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT * THREADS);
    }

    #[test]
    fn check_dedup() {
        const COUNT: usize = 100_000;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();
        let handle = collector.register(None);

        unsafe {
            let guard = &handle.pin();

            for i in 0..COUNT {
                let a = Owned::new(Elem(7i32)).into_shared(guard);
                guard.defer_unchecked(move || a.into_owned(), Some(i));
                guard.defer_unchecked(move || a.into_owned(), Some(i));
            }
            guard.flush();
        }

        while DROPS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.global.collect(guard);
        }

        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT);
    }
}
