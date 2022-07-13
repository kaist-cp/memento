//! Utilities
#![allow(unused)]

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
    use std::io::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::NamedTempFile;

    use crate::pmem::pool::*;
    use crate::pmem::ralloc::{Collectable, GarbageCollection};
    use crate::PDefault;

    #[cfg(feature = "simulate_tcrash")]
    use {
        crate::ploc::NR_MAX_THREADS,
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

    use lazy_static::lazy_static;

    const MAX_THREADS: usize = 64;
    const MAX_COUNT: usize = 1_000_000;
    const NONE: usize = usize::MAX;

    lazy_static! {
        pub static ref JOB_FINISHED: AtomicUsize = AtomicUsize::new(0);
        pub static ref RESULTS: [AtomicUsize; MAX_THREADS] =
            array_init::array_init(|_| AtomicUsize::new(0));
        pub static ref RESULTS_TCRASH: [[AtomicUsize; MAX_COUNT]; MAX_THREADS] =
            array_init::array_init(|_| array_init::array_init(|_| AtomicUsize::new(NONE)));
    }

    #[cfg(feature = "simulate_tcrash")]
    lazy_static! {
        pub static ref UNIX_TIDS: [AtomicI32; NR_MAX_THREADS] =
            array_init::array_init(|_| AtomicI32::new(0));
        pub static ref RESIZE_LOOP_UNIX_TID: AtomicI32 = AtomicI32::new(0);
        pub static ref TEST_FINISHED: AtomicBool = AtomicBool::new(false);
    }
    #[cfg(feature = "simulate_tcrash")]
    const UNIX_TID_FINISH: i32 = i32::MIN;

    /// run test op
    #[allow(box_pointers)]
    pub fn run_test<O, M>(pool_name: &'static str, pool_len: usize, nr_memento: usize)
    where
        O: RootObj<M> + Send + Sync + 'static,
        M: Collectable + Default + Send + Sync,
    {
        #[cfg(not(feature = "simulate_tcrash"))]
        {
            run_test_inner::<O, M>(pool_name, pool_len, nr_memento);
        }

        #[cfg(feature = "simulate_tcrash")]
        {
            // Assertion err causes abort.
            std::panic::always_abort();

            // Install signal handler
            let _ = unsafe { libc::signal(SIGUSR2, texit as size_t) };

            // Start test
            let handle = std::thread::spawn(move || {
                // initialze test variables
                lazy_static::initialize(&JOB_FINISHED);
                lazy_static::initialize(&RESULTS);
                lazy_static::initialize(&RESULTS_TCRASH);
                lazy_static::initialize(&UNIX_TIDS);
                lazy_static::initialize(&TEST_FINISHED);

                run_test_inner::<O, M>(pool_name, pool_len, nr_memento);

                TEST_FINISHED.store(true, Ordering::SeqCst);
            });

            kill_random();
            let _ = handle.join();
        }
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

    /// main thread handler: kill random child thread
    #[cfg(feature = "simulate_tcrash")]
    pub fn kill_random() {
        let pid = unsafe { libc::getpid() };
        loop {
            if TEST_FINISHED.load(Ordering::SeqCst) {
                return;
            }

            let rand_tid = rdtscp() as usize % UNIX_TIDS.len();
            let unix_tid = UNIX_TIDS[rand_tid].load(Ordering::SeqCst);

            if rand_tid > 1 && unix_tid > pid {
                println!("[kill_random] kill t{rand_tid}");

                let _ = UNIX_TIDS[rand_tid]
                    .compare_exchange(unix_tid, -unix_tid, Ordering::SeqCst, Ordering::SeqCst)
                    .map(|_| {
                        let _ = unsafe { libc::syscall(libc::SYS_tgkill, pid, unix_tid, SIGUSR2) };
                    })
                    .map_err(|e| assert!(e == UNIX_TID_FINISH, "tid: {rand_tid}, e: {e}"));
                return;
            }
        }
    }

    /// child thread handler: thread exit
    #[cfg(feature = "simulate_tcrash")]
    pub fn texit(_signum: usize) {
        // NOTE: https://man7.org/linux/man-pages/man7/signal-safety.7.html
        let _ = unsafe { libc::pthread_exit(&0 as *const _ as *mut _) };
    }

    /// Enable being selected by `kill_random` on the main thread
    #[cfg(feature = "simulate_tcrash")]
    pub fn enable_killed(tid: usize) {
        UNIX_TIDS[tid].store(unsafe { libc::gettid() }, Ordering::SeqCst);
    }

    /// Disable being selected by `kill_random` on the main thread
    #[cfg(feature = "simulate_tcrash")]
    pub fn disable_killed(tid: usize) {
        use crossbeam_utils::Backoff;

        // TODO: 리팩토링하며 삭제. checker만을 위한 예외처리임.
        if tid <= 1 {
            return;
        }

        let unix_tid = unsafe { libc::gettid() };
        if let Err(e) =
            UNIX_TIDS[tid].compare_exchange(unix_tid, -1, Ordering::SeqCst, Ordering::SeqCst)
        {
            assert!(e == -unix_tid, "tid: {tid}, e: {e}");

            // Wait until main thread kills me
            let backoff = Backoff::new();
            loop {
                backoff.snooze();
            }
        }
    }

    /// (tid, seq) -> unique repr
    ///
    /// - tid must be less than 100
    pub(crate) fn compose(tid: usize, seq: usize) -> usize {
        seq * 100 + tid
    }

    /// unique repr -> (tid, seq)
    pub(crate) fn decompose(repr: usize) -> (usize, usize) {
        (repr % 100, repr / 100)
    }

    #[allow(unused_variables)]
    pub(crate) fn check_res(tid: usize, nr_wait: usize, count: usize) {
        // Wait for all other threads to finish
        #[cfg(feature = "simulate_tcrash")]
        let my_unix_tid = unsafe { libc::gettid() };
        #[cfg(feature = "simulate_tcrash")]
        let mut cnt = 0;
        while JOB_FINISHED.load(Ordering::SeqCst) < nr_wait {
            #[cfg(feature = "simulate_tcrash")]
            {
                if cnt > 300 {
                    println!("Stop testing. Maybe there is a bug... (1)");
                    unsafe { libc::exit(1) };
                }

                if cnt % 10 == 0 {
                    println!(
                        "[run] t{tid} JOB_FINISHED: {} (unix_tid: {my_unix_tid}, cnt: {cnt})",
                        JOB_FINISHED.load(Ordering::SeqCst)
                    );
                }

                std::thread::sleep(std::time::Duration::from_secs_f64(0.1));
                cnt += 1;
            }
        }

        // Wait until other threads are prevented from being selected by `kill_random` on the main thread.
        // TODO: 테스트할 스레드 번호들을 정확히 안다면, 이 wait 로직 뺄 수 있을듯. 위에서 unix_tid 전부 `UNIX_TID_FINISH`가 될때까지 기다리면 됨.
        #[cfg(feature = "simulate_tcrash")]
        for unix_tid in UNIX_TIDS.iter() {
            if my_unix_tid == unix_tid.load(Ordering::SeqCst) {
                continue;
            }

            loop {
                let unix_tid = unix_tid.load(Ordering::SeqCst);
                if unix_tid <= 0 || unix_tid == RESIZE_LOOP_UNIX_TID.load(Ordering::SeqCst) {
                    break;
                }
            }
        }

        // Check results
        let mut nr_has_res = 0;
        for (tid, result) in RESULTS_TCRASH.iter().enumerate() {
            // check empty
            if result.iter().all(|x| x.load(Ordering::SeqCst) == NONE) {
                continue;
            }

            // check values
            assert!((0..count)
                .map(|seq| seq)
                .all(|seq| result[seq].swap(NONE, Ordering::SeqCst) == get_val(tid, seq)));
            assert!(result.iter().all(|x| x.load(Ordering::SeqCst) == NONE));
            nr_has_res += 1;
        }
        assert!(nr_has_res == nr_wait);
    }

    pub(crate) fn produce_res(tid: usize, seq: usize) {
        let value = get_val(tid, seq);
        let prev = RESULTS_TCRASH[tid][seq].swap(value, Ordering::SeqCst);
        assert!(prev == NONE || prev == value);
    }

    pub(crate) fn get_val(tid: usize, seq: usize) -> usize {
        seq % tid
    }
}
