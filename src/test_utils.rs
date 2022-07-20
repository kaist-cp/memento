//! Utilities

pub(crate) mod ordo {
    use std::{
        mem::{size_of, MaybeUninit},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Barrier,
        },
    };

    use crossbeam_utils::thread;
    use itertools::Itertools;
    use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};

    use crate::pmem::{lfence, rdtscp};

    fn set_affinity(c: usize) {
        unsafe {
            let mut cpuset = MaybeUninit::<cpu_set_t>::zeroed().assume_init();
            CPU_ZERO(&mut cpuset);
            CPU_SET(c, &mut cpuset);
            assert!(sched_setaffinity(0, size_of::<cpu_set_t>(), &cpuset as *const _) >= 0);
        }
    }

    // TODO: sched_setscheduler(getpid(), SCHED_FIFO, param)
    fn clock_offset(c0: usize, c1: usize) -> u64 {
        const RUNS: usize = 100;
        let clock = AtomicU64::new(1);
        let mut min = u64::MAX;

        #[allow(box_pointers)]
        thread::scope(|scope| {
            let clock_ref = &clock;
            let bar0 = Arc::new(Barrier::new(2));
            let bar1 = Arc::clone(&bar0);

            let _ = scope.spawn(move |_| {
                set_affinity(c1);
                for _ in 0..RUNS {
                    while clock_ref.load(Ordering::Relaxed) != 0 {
                        lfence();
                    }
                    clock_ref.store(rdtscp(), Ordering::SeqCst);
                    let _ = bar1.wait();
                }
            });

            let h = scope.spawn(move |_| {
                set_affinity(c0);
                let mut min = u64::MAX;
                for _ in 0..RUNS {
                    clock_ref.store(0, Ordering::SeqCst);
                    let t = loop {
                        let t = clock_ref.load(Ordering::Relaxed);
                        if t != 0 {
                            break t;
                        }
                        lfence();
                    };
                    min = min.min(rdtscp().abs_diff(t));
                    let _ = bar0.wait();
                }
                min
            });

            min = h.join().unwrap();
        })
        .unwrap();

        min
    }

    pub(crate) fn get_ordo_boundary() -> u64 {
        let num_cpus = num_cpus::get();
        let mut global_offset = 0;

        for c in (0..num_cpus).combinations(2) {
            global_offset =
                global_offset.max(clock_offset(c[0], c[1]).max(clock_offset(c[1], c[0])));
        }
        global_offset
    }
}

#[doc(hidden)]
pub mod tests {
    #![allow(dead_code)]

    use crossbeam_epoch::Guard;
    use crossbeam_utils::Backoff;
    use std::io::Error;
    use std::sync::atomic::{fence, AtomicUsize, Ordering};
    use tempfile::NamedTempFile;

    use crate::pmem::pool::*;
    use crate::pmem::ralloc::{Collectable, GarbageCollection};
    use crate::PDefault;

    use {
        crate::pmem::rdtscp,
        libc::{size_t, SIGUSR2},
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

    #[derive(Debug)]
    pub struct DummyRootObj;

    impl Collectable for DummyRootObj {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
            // no-op
        }
    }

    impl PDefault for DummyRootObj {
        fn pdefault(_: &PoolHandle) -> Self {
            Self {}
        }
    }

    impl RootObj<DummyRootMemento> for DummyRootObj {
        fn run(&self, _: &mut DummyRootMemento, _: usize, _: &Guard, _: &PoolHandle) {
            // no-op
        }
    }

    #[derive(Debug, Default)]
    pub struct DummyRootMemento;

    impl Collectable for DummyRootMemento {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
            // no-op
        }
    }

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

    pub(crate) struct TestRootObj<O: PDefault + Collectable> {
        pub(crate) obj: O,
    }

    impl<O: PDefault + Collectable> PDefault for TestRootObj<O> {
        fn pdefault(pool: &PoolHandle) -> Self {
            Self {
                obj: O::pdefault(pool),
            }
        }
    }

    impl<O: PDefault + Collectable> Collectable for TestRootObj<O> {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            O::filter(&mut s.obj, tid, gc, pool)
        }
    }

    pub static mut TESTER: Option<Tester> = None;

    /// run test op
    #[allow(box_pointers)]
    pub fn run_test<O, M>(
        pool_name: &'static str,
        pool_len: usize,
        nr_memento: usize,
        nr_count: usize,
    ) where
        O: RootObj<M> + Send + Sync + 'static,
        M: Collectable + Default + Send + Sync,
    {
        // Assertion err causes abort.
        std::panic::always_abort();

        // Install signal handler
        let _ = unsafe { libc::signal(SIGUSR2, texit as size_t) };

        // Initialize tester
        unsafe { TESTER = Some(Tester::new(nr_memento, nr_count)) };
        let tester = unsafe { TESTER.as_ref().unwrap() };
        fence(Ordering::SeqCst);

        // Start test
        let handle = std::thread::spawn(move || {
            run_test_inner::<O, M>(pool_name, pool_len, nr_memento);
        });

        tester.kill();
        let _ = handle.join();

        // Check test results
        tester.check();
    }

    pub fn run_test_inner<O, M>(pool_name: &str, pool_len: usize, nr_memento: usize)
    where
        O: RootObj<M> + Send + Sync + 'static,
        M: Collectable + Default + Send + Sync,
    {
        let filepath = get_test_abs_path(pool_name);

        // remove pool
        // let _ = Pool::remove(&filepath);

        // open pool
        let pool_handle = unsafe { Pool::open::<O, M>(&filepath, pool_len) }
            .unwrap_or_else(|_| Pool::create::<O, M>(&filepath, pool_len, nr_memento).unwrap());

        // run root memento(s)
        pool_handle.execute::<O, M>();
    }

    /// child thread handler: thread exit
    pub fn texit(_signum: usize) {
        // NOTE: https://man7.org/linux/man-pages/man7/signal-safety.7.html
        let _ = unsafe { libc::pthread_exit(&0 as *const _ as *mut _) };
    }

    #[derive(Debug, Clone, Copy)]
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
        fn into_usize(self) -> usize {
            self.data
        }
    }

    impl Collectable for TestValue {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
    }

    #[derive(Debug)]
    pub struct Testee<'a> {
        info: &'a TestInfo,
    }

    impl<'a> Testee<'a> {
        #[inline]
        pub fn report(&self, seq: usize, val: TestValue) {
            self.info.report(seq, val)
        }
    }

    impl<'a> Drop for Testee<'a> {
        fn drop(&mut self) {
            self.info.finish();
        }
    }

    #[derive(Debug)]
    struct TestInfo {
        state: AtomicI32,
        crash_seq: usize,
        is_testee: AtomicBool,
        results: [AtomicUsize; Self::MAX_COUNT],
    }

    impl TestInfo {
        const MAX_COUNT: usize = 1_000_000;

        const RESULT_INIT: usize = 0;

        const STATE_INIT: i32 = 0;
        const STATE_KILLED: i32 = -1;
        const STATE_FINISHED: i32 = i32::MAX;

        fn new(nr_count: usize) -> Self {
            let crash_seq;
            #[cfg(not(feature = "no_crash_test"))]
            {
                crash_seq = rdtscp() as usize % nr_count;
            }

            #[cfg(feature = "no_crash_test")]
            {
                crash_seq = usize::MAX;
            }

            Self {
                state: AtomicI32::new(Self::STATE_INIT),
                crash_seq,
                is_testee: AtomicBool::new(false),
                results: array_init::array_init(|_| AtomicUsize::new(Self::RESULT_INIT)),
            }
        }

        fn report(&self, seq: usize, val: TestValue) {
            let val = val.into_usize();
            let prev = self.results[seq].swap(val, Ordering::SeqCst);
            assert!(prev == Self::RESULT_INIT || prev == val);

            if self.crash_seq == seq {
                self.enable_killed();
            }
        }

        /// Enable being selected by `kill()`
        // TODO: How to kill resizer in clevel?
        #[inline]
        fn enable_killed(&self) {
            let unix_tid = unsafe { libc::gettid() };
            self.state.store(unix_tid, Ordering::SeqCst);
        }

        fn finish(&self) {
            let unix_tid = unsafe { libc::gettid() };
            if self
                .state
                .compare_exchange(
                    unix_tid,
                    Self::STATE_FINISHED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return;
            }

            let backoff = Backoff::default();
            while self.state.load(Ordering::SeqCst) == Self::STATE_KILLED {
                // Wait until main thread kills tid
                backoff.snooze();
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
        const MAX_THREAD: usize = 100;

        fn new(nr_thread: usize, nr_count: usize) -> Self {
            Self {
                infos: array_init::array_init(|_| TestInfo::new(nr_count)),
                nr_thread,
                nr_count,
            }
        }

        pub fn testee<'a>(&'a self, tid: usize) -> Testee<'a> {
            let inner_tid = tid - 1;
            let info = &self.infos[inner_tid];

            info.state.store(TestInfo::STATE_INIT, Ordering::SeqCst);
            info.is_testee.store(true, Ordering::SeqCst);
            Testee { info }
        }

        /// Kill arbitrary child thread
        pub(crate) fn kill(&self) {
            let pid = unsafe { libc::getpid() };
            let backoff = Backoff::new();

            loop {
                let mut done = true;
                for (tid, state) in (0..self.nr_thread)
                    .map(|tid| (tid, self.infos[tid].state.load(Ordering::SeqCst)))
                {
                    if state == TestInfo::STATE_FINISHED {
                        continue;
                    }

                    done = false;

                    if state == TestInfo::STATE_INIT {
                        continue;
                    }

                    let unix_tid = state;

                    if let Err(e) = self.infos[tid].state.compare_exchange(
                        unix_tid,
                        TestInfo::STATE_KILLED,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        assert_eq!(e, TestInfo::STATE_FINISHED);
                    } else {
                        println!("[Tester] Killing t{}", tid + 1);
                        let _ = unsafe { libc::syscall(libc::SYS_tgkill, pid, unix_tid, SIGUSR2) };
                        self.infos[tid]
                            .state
                            .store(TestInfo::STATE_INIT, Ordering::SeqCst);
                        return;
                    }
                }

                if done {
                    println!("[Tester] No kill");
                    return;
                }

                backoff.snooze();
            }
        }

        fn check(&self) {
            // // Wait for all other threads to finish
            // #[cfg(feature = "simulate_tcrash")]
            // let my_unix_tid = unsafe { libc::gettid() };
            // #[cfg(feature = "simulate_tcrash")]
            // let mut cnt = 0;
            // while JOB_FINISHED.load(Ordering::SeqCst) < NR_THREAD {
            //     #[cfg(feature = "simulate_tcrash")]
            //     {
            //         if cnt > 300 {
            //             println!("Stop testing. Maybe there is a bug... (1)");
            //             unsafe { libc::exit(1) };
            //         }

            //         if cnt % 10 == 0 {
            //             let nr_finished = JOB_FINISHED.load(Ordering::SeqCst);
            //             println!("[run] t{tid} JOB_FINISHED: {nr_finished} (unix_tid: {my_unix_tid}, cnt: {cnt})");
            //         }

            //         std::thread::sleep(std::time::Duration::from_secs_f64(0.1));
            //         cnt += 1;
            //     }
            // }

            let mut checked_map = vec![vec![false; self.nr_count]; self.nr_thread + 1];

            for results in self.infos.iter().filter_map(|info| {
                info.is_testee
                    .load(Ordering::SeqCst)
                    .then_some(&info.results)
            }) {
                for result in (0..self.nr_count).map(|i| results[i].load(Ordering::SeqCst)) {
                    assert_ne!(result, TestInfo::RESULT_INIT);
                    let (tid, seq) = TestValue::decompose(TestValue { data: result });
                    assert!(!checked_map[tid][seq]);
                    checked_map[tid][seq] = true;
                }
            }
        }
    }

    impl Drop for Tester {
        fn drop(&mut self) {
            self.check();
        }
    }
}
