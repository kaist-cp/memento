//! The default garbage collector.
//!
//! For each thread, a participant is lazily initialized on its first use, when the current thread
//! is registered in the default collector.  If initialized, the thread's participant will get
//! destructed on thread exit, which in turn unregisters the thread.

use std::cell::RefCell;

use crate::collector::{Collector, LocalHandle};
use crate::guard::Guard;
use crate::primitive::{lazy_static, thread_local};

lazy_static! {
    /// The global data for the default garbage collector.
    static ref COLLECTOR: Collector = Collector::new();
}

thread_local! {
    /// The per-thread participant for the default garbage collector.
    static HANDLE: RefCell<LocalHandle> = RefCell::new(COLLECTOR.register(None));
}

/// Returns the guard used by the `tid` thread
///
/// # Safety
///
/// Each `tid' should be used by only one thread.
/// For example, two threads should not both call old guard with tid 0.
pub unsafe fn old_guard(tid: usize) -> Guard {
    HANDLE.with(|h| {
        // Find the `Local` used by `tid` thread
        if let Some(handle) = COLLECTOR.find(tid) {
            // If it crashes during repin, we must ensure that there is a guard. See comments in `repin_after()`.
            if handle.is_repinning() {
                unsafe { handle.set_guard_count(1) }
            }

            // Creating a guard with previous context
            let guard = handle.pin();

            // Re-initialize the number of objs counted by `Local`.
            // Since all the obj (`LocalHandle`, `Guard`) derived from Local that existed before are all gone, you need to initialize the number of obj counted by local well.
            unsafe { handle.reset_count() };
            unsafe { handle.set_guard_count(1) };
            h.replace(handle);
            return guard;
        }

        // If there is no `Local` used by `tid` thread, register a new one.
        h.replace(COLLECTOR.register(Some(tid)));
        return h.borrow().pin();
    })
}

/// Pins the current thread.
#[inline]
pub fn pin() -> Guard {
    with_handle(|handle| handle.borrow().pin())
}

/// Returns `true` if the current thread is pinned.
#[inline]
pub fn is_pinned() -> bool {
    with_handle(|handle| handle.borrow().is_pinned())
}

/// Returns the default global collector.
pub fn default_collector() -> &'static Collector {
    &COLLECTOR
}

#[inline]
fn with_handle<F, R>(mut f: F) -> R
where
    F: FnMut(&RefCell<LocalHandle>) -> R,
{
    HANDLE
        .try_with(|h| f(h))
        .unwrap_or_else(|_| f(&RefCell::new(COLLECTOR.register(None))))
}

#[cfg(all(test, not(crossbeam_loom)))]
mod tests {
    use crate::{pin, unprotected, Owned};
    use crossbeam_utils::thread;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn pin_while_exiting() {
        struct Foo;

        impl Drop for Foo {
            fn drop(&mut self) {
                // Pin after `HANDLE` has been dropped. This must not panic.
                super::pin();
            }
        }

        thread_local! {
            static FOO: Foo = Foo;
        }

        thread::scope(|scope| {
            scope.spawn(|_| {
                // Initialize `FOO` and then `HANDLE`.
                FOO.with(|_| ());
                super::pin();
                // At thread exit, `HANDLE` gets dropped first and `FOO` second.
            });
        })
        .unwrap();
    }

    // Test 3 properties of old guard
    // 1. Check if the guard doesn't drop in case of abnormal termination (i.e. thread-local panic)
    // 2. Check if the preserved guard can be successfully brought back to old_guard
    // 3. Check if the guard drops well at normal termination.
    #[test]
    fn old_guard() {
        use crate::{default_collector, old_guard};
        const THREADS: usize = 4;
        const COUNT: usize = 64;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem {}
        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        #[allow(box_pointers)]
        thread::scope(|scope| {
            // Phase 1.
            // Thread 0 dies after holding the guard. But the guard is preserved.
            let handler = scope.spawn(move |_| {
                let _guard = unsafe { old_guard(0) };
                panic!();
            });
            let _ = handler.join();

            // Phase 2.
            // Other threads defer the elem drop several times, but since the guard held by thread 0 remains, no deferred function can be executed.
            let mut handlers = Vec::new();
            for _ in 1..THREADS {
                let h = scope.spawn(move |_| {
                    for _ in 0..COUNT {
                        let guard = &pin();
                        let elem = Owned::new(Elem {}).into_shared(guard);
                        unsafe { guard.defer_destroy(elem) }
                        guard.flush();
                    }
                });
                handlers.push(h);
            }

            while !handlers.is_empty() {
                let _ = handlers.pop().unwrap().join();
                let guard = &pin();
                default_collector().global.collect(guard);
                assert_eq!(DROPS.load(Ordering::Relaxed), 0);
            }

            // Phase 3.
            // If thread 0 brings back the guard and ends normally, this time it drops well without preserving the guard.
            // Now that all guards are gone, the reserved function can be called (i.e. global epoch can be advanced)
            let handler = scope.spawn(move |_| {
                let _guard = unsafe { old_guard(0) };
            });
            let _ = handler.join();

            // Advance and collect so that deferred drop is called
            // The reason we do this directly is that advance and collect are called only when we make a new pin.
            default_collector()
                .global
                .try_advance(unsafe { unprotected() });
            default_collector().global.collect(unsafe { unprotected() });
            assert!(DROPS.load(Ordering::Relaxed) != 0);

            // If you use pin instead of old_guard, you can see that elem drop is not delayed and called in phase 2
        })
        .unwrap();
    }
}
