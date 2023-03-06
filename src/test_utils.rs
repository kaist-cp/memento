//! Utilities

pub(crate) mod thread {
    use std::{any::TypeId, marker::PhantomData};

    use libc::c_void;

    pub(crate) struct JoinHandle<T> {
        native: u64,
        phantom: PhantomData<T>,
    }

    impl<T: 'static> JoinHandle<T> {
        #[allow(box_pointers)]
        pub(crate) fn join(&self) -> Result<T, ()> {
            let mut status = std::ptr::null_mut();
            if unsafe { libc::pthread_join(self.native, &mut status) } == 0 {
                // no return value
                if TypeId::of::<()>() == TypeId::of::<T>() {
                    if status as *const _ as usize == 0 {
                        Ok(unsafe { std::mem::transmute_copy(&()) })
                    } else {
                        Err(())
                    }
                }
                // there is return value
                else {
                    Ok(*unsafe { Box::from_raw(status as *mut _ as *mut T) })
                }
            } else {
                Err(())
            }
        }
    }

    #[allow(box_pointers)]
    pub(crate) fn spawn<'a, F, T>(f: F) -> JoinHandle<T>
    where
        F: Fn() -> T,
        F: Send + 'a,
        T: Send + 'static,
    {
        extern "C" fn func<T>(main: *mut c_void) -> *mut c_void
        where
            T: Send + 'static,
        {
            let res = unsafe { Box::from_raw(main as *mut Box<dyn FnOnce() -> T>)() };
            // no return value
            if TypeId::of::<()>() == TypeId::of::<T>() {
                std::ptr::null_mut() as *mut _ as *mut c_void
            }
            // return value should be delivered by dynamic allocaiton.
            else {
                Box::into_raw(Box::new(res)) as *mut _ as *mut c_void
            }
        }

        // Initialize thread attributes.
        let mut native: libc::pthread_t = unsafe { std::mem::zeroed() };
        let mut attr: libc::pthread_attr_t = unsafe { std::mem::zeroed() };
        unsafe {
            assert_eq!(libc::pthread_attr_init(&mut attr), 0);

            // Set stack size.
            if let Some(stacksize) = std::env::var("RUST_MIN_STACK")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                // Round up to the nearest page to prevent error.
                // https://man7.org/linux/man-pages/man3/pthread_attr_setstacksize.3.html#ERRORS
                let pagesize = libc::sysconf(libc::_SC_PAGESIZE) as usize;
                let stacksize = std::cmp::max(
                    libc::PTHREAD_STACK_MIN,
                    (stacksize + pagesize - 1) & (-(pagesize as isize - 1) as usize - 1),
                );
                assert_eq!(libc::pthread_attr_setstacksize(&mut attr, stacksize), 0);
            }
        }

        let main = move || f();
        let p = unsafe {
            std::mem::transmute::<Box<dyn FnOnce() -> T + 'a>, Box<dyn FnOnce() + 'a>>(Box::new(
                main,
            ))
        };
        let p = Box::into_raw(Box::new(p));
        unsafe {
            let _err = libc::pthread_create(&mut native, &attr, func::<T>, p as *mut _);
            // let _err = libc::pthread_create(&mut native, &attr, func::<T>, p as *mut _);
        }
        unsafe { assert_eq!(libc::pthread_attr_destroy(&mut attr), 0) };

        JoinHandle {
            native,
            phantom: PhantomData,
        }
    }

    #[test]
    fn join_retval() {
        assert_eq!(spawn(move || 3 + 5).join().unwrap(), 8);
        assert_eq!(spawn(move || 5 * 15 + 3).join().unwrap(), 5 * 15 + 3);
    }

    #[test]
    fn spawn_params() {
        let a = 10;
        let b = 20;
        let c = 30;
        let d = 40;
        let _ = spawn(move || {
            assert_eq!(a, 10);
        });
        let _ = spawn(move || {
            assert_eq!(a, 10);
            assert_eq!(a, 10);
        });
        let _ = spawn(move || {
            assert_eq!(a, 10);
            assert_eq!(a, 10);
            assert_eq!(a, 10);
            assert_eq!(a, 10);
        });
        let _ = spawn(move || {
            assert_eq!(a, 10);
            assert_eq!(a, 10);
            assert_eq!(b, 20);
            assert_eq!(b, 20);
        });
        let _ = spawn(move || {
            assert_eq!(a, 10);
            assert_eq!(b, 20);
            assert_eq!(c, 30);
            assert_eq!(d, 40);
        });
    }
}

#[doc(hidden)]
#[allow(warnings)]
pub mod tests {
    use atomic::Atomic;
    use crossbeam_utils::Backoff;
    use mmt_derive::Collectable;

    #[cfg(feature = "tcrash")]
    use {
        libc::{size_t, SIGUSR2},
        std::backtrace::Backtrace,
    };

    use std::io::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::NamedTempFile;

    use crate::ploc::Handle;
    use crate::pmem::alloc::{Collectable, GarbageCollection};
    use crate::pmem::pool::*;
    use crate::test_utils::thread;
    use crate::{Memento, PDefault};

    use {
        crate::pmem::*,
        std::sync::atomic::{AtomicBool, AtomicI32},
    };

    /// get path for test file
    ///
    /// e.g. "foo.pool" => "{project-path}/test/foo.pool"
    pub fn get_test_abs_path(rel_path: &str) -> String {
        let mut path = std::path::PathBuf::new();
        #[cfg(not(feature = "no_persist"))]
        {
            path.push("/mnt/pmem0")
        }
        #[cfg(feature = "no_persist")]
        {
            path.push(env!("CARGO_MANIFEST_DIR")); // project path
        }
        path.push("test");
        path.push(rel_path);
        path.push(rel_path.to_string() + ".pool");
        path.to_str().unwrap().to_string()
    }

    #[derive(Debug, Collectable)]
    pub struct DummyRootObj;

    impl PDefault for DummyRootObj {
        fn pdefault(_: &Handle) -> Self {
            Self {}
        }
    }

    impl RootObj<DummyRootMemento> for DummyRootObj {
        fn run(&self, _: &mut DummyRootMemento, _: &Handle) {
            // no-op
        }
    }

    #[derive(Debug, Default, Memento, Collectable)]
    pub struct DummyRootMemento;

    /// get dummy pool handle for test
    pub fn get_dummy_handle(filesize: usize) -> Result<&'static PoolHandle, Error> {
        #[cfg(not(feature = "no_persist"))]
        {
            let temp_path = NamedTempFile::new_in("/mnt/pmem0")?
                .path()
                .to_str()
                .unwrap()
                .to_owned();

            Pool::create::<DummyRootObj, DummyRootMemento>(&temp_path, filesize, 0)
        }
        #[cfg(feature = "no_persist")]
        {
            let temp_path = NamedTempFile::new()?.path().to_str().unwrap().to_owned();
            Pool::create::<DummyRootObj, DummyRootMemento>(&temp_path, filesize, 0)
        }
    }

    #[derive(Collectable)]
    pub(crate) struct TestRootObj<O: PDefault + Collectable> {
        pub(crate) obj: O,
    }

    impl<O: PDefault + Collectable> PDefault for TestRootObj<O> {
        fn pdefault(handle: &Handle) -> Self {
            Self {
                obj: O::pdefault(handle),
            }
        }
    }

    pub static mut TESTER: Option<Tester> = None;
    pub static mut TESTER_FLAG: AtomicBool = AtomicBool::new(false);

    /// run test op
    #[allow(box_pointers)]
    pub fn run_test<O, M>(
        pool_name: &'static str,
        pool_len: usize,
        nr_memento: usize,
        nr_count: usize,
    ) where
        O: RootObj<M> + Send + Sync + 'static,
        M: Memento + Send + Sync,
    {
        #[cfg(feature = "tcrash")]
        {
            // Assertion err causes abort.
            std::panic::set_hook(Box::new(|info| {
                println!("Thread {} {info}", unsafe { libc::gettid() });
                println!("{}", Backtrace::capture());
                unsafe { libc::abort() };
            }));

            // Install signal handler
            let _ = unsafe { libc::signal(SIGUSR2, texit as size_t) };
        }

        // Initialize tester
        let tester = unsafe {
            TESTER = Some(Tester::new(nr_memento, nr_count));
            TESTER_FLAG.store(true, Ordering::Release);
            TESTER.as_ref().unwrap()
        };

        // Start test
        let handle = thread::spawn(move || {
            run_test_inner::<O, M>(pool_name, pool_len, nr_memento);
        });

        #[cfg(feature = "tcrash")]
        tester.kill();

        let _ = handle.join();

        // Check test results
        #[cfg(not(feature = "pmcheck"))] // TODO: Remove
        tester.check();
    }

    pub fn run_test_inner<O, M>(pool_name: &str, pool_len: usize, nr_memento: usize)
    where
        O: RootObj<M> + Send + Sync + 'static,
        M: Memento + Send + Sync,
    {
        let filepath = get_test_abs_path(pool_name);

        // remove pool
        // let _ = Pool::remove(&filepath);

        // open pool
        let pool_handle = unsafe { Pool::open::<O, M>(&filepath, pool_len) }.unwrap_or_else(|_| {
            let _ = Pool::remove(&filepath);
            Pool::create::<O, M>(&filepath, pool_len, nr_memento).unwrap()
        });

        // run root memento(s)
        pool_handle.execute::<O, M>();
    }

    /// child thread handler: thread exit
    pub fn texit(_signum: usize) {
        // NOTE: https://man7.org/linux/man-pages/man7/signal-safety.7.html
        let _ = std::rt::panic_count::increase();
        unsafe { libc::pthread_exit(&0 as *const _ as *mut _) };
    }

    #[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Collectable)]
    pub struct TestValue {
        data: usize,
    }

    impl TestValue {
        const TID_LIMIT: usize = 100;

        /// (tid, seq) -> unique repr
        ///
        /// - tid must be less than TID_LIMIT
        #[inline]
        pub fn new(tid: usize, seq: usize) -> Self {
            Self::compose(tid, seq)
        }

        #[inline]
        fn compose(tid: usize, seq: usize) -> Self {
            Self {
                data: seq * Self::TID_LIMIT + tid,
            }
        }

        /// unique repr -> (tid, seq)
        #[inline]
        fn decompose(repr: Self) -> (usize, usize) {
            (repr.data % Self::TID_LIMIT, repr.data / Self::TID_LIMIT)
        }

        #[inline]
        pub fn into_usize(self) -> usize {
            self.data
        }

        #[inline]
        pub fn from_usize(data: usize) -> Self {
            Self { data }
        }
    }

    #[derive(Debug)]
    pub struct Testee<'a> {
        info: &'a TestInfo,
    }

    impl Testee<'_> {
        #[inline]
        pub fn report(&self, seq: usize, val: TestValue) {
            self.info.report(seq, val)
        }
    }

    impl Drop for Testee<'_> {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                self.info.finish();
                println!("[Testee {}] Finished.", self.info.tid);
            } else {
                println!("[Testee {}] Killed.", self.info.tid);
            }
        }
    }

    #[derive(Debug)]
    struct TestInfo {
        tid: usize,
        state: AtomicI32,
        checked: Atomic<Option<bool>>,
        results: [AtomicUsize; Self::MAX_COUNT],
        crash_seq: usize,
    }

    impl TestInfo {
        const MAX_COUNT: usize = 1_000_000;

        const RESULT_INIT: usize = 0;

        const STATE_INIT: i32 = 0;
        const STATE_KILLED: i32 = -1;
        const STATE_FINISHED: i32 = i32::MAX;

        fn new(tid: usize, nr_count: usize) -> Self {
            assert!(nr_count <= Self::MAX_COUNT);

            Self {
                tid,
                state: AtomicI32::new(Self::STATE_INIT),
                checked: Atomic::new(None),
                results: array_init::array_init(|_| AtomicUsize::new(Self::RESULT_INIT)),
                crash_seq: rdtscp() as usize % nr_count,
            }
        }

        fn report(&self, seq: usize, val: TestValue) {
            let uval = val.into_usize();
            let prev = self.results[seq].swap(uval, Ordering::SeqCst);
            assert!(
                prev == Self::RESULT_INIT || prev == uval,
                "prev: {prev}, val: {uval}"
            );

            if self.crash_seq == seq {
                self.enable_killed();
            }
        }

        /// Enable being selected by `kill()`
        #[inline]
        fn enable_killed(&self) {
            let unix_tid = unsafe { libc::gettid() };
            self.state.store(unix_tid, Ordering::SeqCst);
        }

        fn finish(&self) {
            let unix_tid = unsafe { libc::gettid() };
            if let Err(e) = self.state.compare_exchange(
                unix_tid,
                Self::STATE_FINISHED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                assert_eq!(e, Self::STATE_KILLED);

                let backoff = Backoff::default();
                loop {
                    // Wait until main thread kills tid
                    backoff.snooze();
                }
            }
        }
    }

    #[derive(Debug)]
    pub struct Tester {
        infos: [TestInfo; Self::MAX_THREAD],
        nr_thread: usize,
        nr_count: usize,
    }

    impl Tester {
        const MAX_THREAD: usize = 30;

        fn new(nr_thread: usize, nr_count: usize) -> Self {
            assert!(nr_thread <= Self::MAX_THREAD);

            Self {
                infos: array_init::array_init(|tid| TestInfo::new(tid + 1, nr_count)),
                nr_thread,
                nr_count,
            }
        }

        pub fn testee(&self, checked: bool, handle: &Handle) -> Testee<'_> {
            let tid = handle.tid;
            let inner_tid = tid - 1;
            let info = &self.infos[inner_tid];

            info.checked.store(Some(checked), Ordering::SeqCst);
            if checked {
                info.state.store(TestInfo::STATE_INIT, Ordering::SeqCst);
            } else {
                info.enable_killed();
            }

            #[cfg(feature = "tcrash")]
            if checked {
                println!(
                    "[Testee {tid}] Crash may occur after seq {}.",
                    info.crash_seq
                );
            }

            Testee { info }
        }

        fn is_started(&self) -> bool {
            (0..self.nr_thread).all(|tid| self.infos[tid].checked.load(Ordering::SeqCst).is_some())
        }

        pub fn is_finished(&self) -> bool {
            self.is_started()
                && (0..self.nr_thread)
                    .filter(|tid| self.infos[*tid].checked.load(Ordering::SeqCst).unwrap())
                    .all(|tid| {
                        self.infos[tid].state.load(Ordering::SeqCst) == TestInfo::STATE_FINISHED
                    })
        }

        /// Kill arbitrary child thread
        #[cfg(feature = "tcrash")]
        pub(crate) fn kill(&self) {
            let pid = unsafe { libc::getpid() };
            let backoff = Backoff::new();

            loop {
                for (tid, unix_tid, checked) in (0..self.nr_thread)
                    .map(|tid| {
                        (
                            tid,
                            self.infos[tid].state.load(Ordering::SeqCst),
                            self.infos[tid].checked.load(Ordering::SeqCst),
                        )
                    })
                    .filter(|(_, state, checked)| {
                        checked.is_some()
                            && *state != TestInfo::STATE_INIT
                            && *state != TestInfo::STATE_FINISHED
                    })
                {
                    if !checked.unwrap() && rdtscp() % 1_000_000 != 0 {
                        continue;
                    }

                    if self.infos[tid]
                        .state
                        .compare_exchange(
                            unix_tid,
                            TestInfo::STATE_KILLED,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        println!(
                            "[Tester] Killing t{} (checked: {})",
                            tid + 1,
                            checked.unwrap()
                        );
                        let _ = unsafe { libc::syscall(libc::SYS_tgkill, pid, unix_tid, SIGUSR2) };
                        return;
                    }
                }

                if self.is_finished() {
                    println!("[Tester] No kill");
                    return;
                }

                backoff.snooze();
            }
        }

        fn check(&self) {
            let mut checked_map = vec![vec![false; self.nr_count]; self.nr_thread + 1];

            for (to_tid, results) in self.infos.iter().filter_map(|info| {
                info.checked
                    .load(Ordering::SeqCst)
                    .unwrap_or_default()
                    .then_some((info.tid, &info.results))
            }) {
                for (to_seq, result) in (0..self.nr_count)
                    .map(|i| results[i].load(Ordering::SeqCst))
                    .enumerate()
                {
                    // `to_tid` must have returned value at `to_seq`
                    assert_ne!(result, TestInfo::RESULT_INIT, "tid:{to_tid}, seq:{to_seq}");

                    // `from_tid`'s `from_seq` must be issued exactly once
                    let (from_tid, from_seq) = TestValue::decompose(TestValue { data: result });
                    assert!(
                        !checked_map[from_tid][from_seq],
                        "From: (tid:{from_tid}, seq:{from_seq} / To: (tid:{to_tid}, seq:{to_seq}",
                    );
                    checked_map[from_tid][from_seq] = true;
                }
            }

            println!("[Tester] Test passed.");
        }
    }
}

pub(crate) mod distributer {
    use std::sync::atomic::AtomicUsize;

    use atomic::Ordering;
    use itertools::Itertools;

    pub(crate) struct Distributer<const NR_THREAD: usize, const NR_COUNT: usize> {
        items: [[AtomicUsize; NR_COUNT]; NR_THREAD],
    }

    impl<const NR_THREAD: usize, const NR_COUNT: usize> Distributer<NR_THREAD, NR_COUNT> {
        const NONE: usize = 0;
        const PRODUCED: usize = usize::MAX;

        pub(crate) fn new() -> Self {
            Self {
                items: array_init::array_init(|_| {
                    array_init::array_init(|_| AtomicUsize::new(Self::NONE))
                }),
            }
        }

        /// Mark items[tid][seq] as produced so that other thread can consume this.
        pub(crate) fn produce(&self, tid: usize, seq: usize) -> bool {
            self.items[tid][seq]
                .compare_exchange(
                    Self::NONE,
                    Self::PRODUCED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
        }

        /// Mark items[tid][seq] as consumed.
        pub(crate) fn consume(&self, tid: usize, seq: usize) -> Option<(usize, usize)> {
            // check if there is already marked by me
            for t in 0..NR_THREAD {
                if self.items[t][seq].load(Ordering::SeqCst) == tid {
                    return Some((t, seq));
                }
            }
            // mark as consumed
            let mut tids = (0..NR_THREAD).collect_vec();
            tids.rotate_left(tid); // start from tid+1
            for t in tids {
                if self.items[t][seq]
                    .compare_exchange(Self::PRODUCED, tid, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Some((t, seq));
                }
            }
            return None;
        }
    }
}
