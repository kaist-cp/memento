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
    use crossbeam_epoch::Guard;
    use std::collections::HashSet;
    use std::io::Error;
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use tempfile::NamedTempFile;

    use crate::pmem::pool::*;
    use crate::pmem::ralloc::{Collectable, GarbageCollection};
    use crate::PDefault;

    #[cfg(feature = "simulate_tcrash")]
    use {
        crate::ploc::NR_MAX_THREADS,
        libc::{gettid, size_t, SIGUSR1, SIGUSR2},
        std::sync::atomic::{AtomicBool, AtomicI32, Ordering},
    };

    /// get path for test file
    ///
    /// e.g. "foo.pool" => "{project-path}/test/foo.pool"
    pub fn get_test_abs_path<P: AsRef<Path>>(rel_path: P) -> String {
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

    lazy_static! {
        pub static ref JOB_FINISHED: AtomicUsize = AtomicUsize::new(0);
        pub static ref RESULTS: [AtomicUsize; 1024] =
            array_init::array_init(|_| AtomicUsize::new(0)); // TODO: 모든 테스트를 RESULTS_TCRASH로 대체하고 이 변수는 삭제
        pub static ref RESULTS_TCRASH: [Mutex<HashSet<usize>>; 1024] =
            array_init::array_init(|_| Mutex::new(HashSet::new()));
    }

    #[cfg(feature = "simulate_tcrash")]
    lazy_static! {
        pub static ref UNIX_TIDS: [AtomicI32; NR_MAX_THREADS] =
            array_init::array_init(|_| AtomicI32::new(0));
        pub static ref TEST_STARTED: AtomicBool = AtomicBool::new(false);
        pub static ref TEST_FINISHED: AtomicBool = AtomicBool::new(false);
    }

    /// run test op
    pub fn run_test<O, M, P>(pool_name: P, pool_len: usize, nr_memento: usize)
    where
        O: RootObj<M> + Send + Sync + 'static,
        M: Collectable + Default + Send + Sync,
        P: AsRef<Path>,
    {
        // initialze test variables
        lazy_static::initialize(&JOB_FINISHED);
        lazy_static::initialize(&RESULTS);
        lazy_static::initialize(&RESULTS_TCRASH);
        #[cfg(feature = "simulate_tcrash")]
        {
            lazy_static::initialize(&UNIX_TIDS);
            lazy_static::initialize(&TEST_STARTED);
            lazy_static::initialize(&TEST_FINISHED);

            println!("Install `kill_random` and `self_panic` handler");
            let _ = unsafe { libc::signal(SIGUSR1, kill_random as size_t) };
            let _ = unsafe { libc::signal(SIGUSR2, self_panic as size_t) };
        }

        let filepath = get_test_abs_path(pool_name);

        // remove pool
        // let _ = Pool::remove(&filepath);

        // open pool
        let pool_handle = unsafe { Pool::open::<O, M>(&filepath, pool_len) }
            .unwrap_or_else(|_| Pool::create::<O, M>(&filepath, pool_len, nr_memento).unwrap());

        // run root memento(s)
        let execute = std::env::var("POOL_EXECUTE");
        if execute.is_ok() && execute.unwrap() == "0" {
            println!("[run_test] no execute");
        } else {
            println!("[run_test] execute");
            pool_handle.execute::<O, M>();
        }

        #[cfg(feature = "simulate_tcrash")]
        TEST_FINISHED.store(true, Ordering::SeqCst);
    }

    /// main thread handler: kill random child thread
    #[cfg(feature = "simulate_tcrash")]
    pub fn kill_random() {
        let pid = unsafe { libc::getpid() };
        loop {
            // it prevents an infinity loop that occurs when the main thread receives a signal right after installing the handler but before spawning child threads.
            if !TEST_STARTED.load(Ordering::SeqCst) {
                println!("[kill_random] No one killed. Because test is not yet started.");
                return;
            }
            if TEST_FINISHED.load(Ordering::SeqCst) {
                println!("[kill_random] No one killed. Because test was already finished.");
                return;
            }

            let rand_tid = rand::random::<usize>() % UNIX_TIDS.len();
            let unix_tid = UNIX_TIDS[rand_tid].load(Ordering::SeqCst);

            if unix_tid > pid {
                println!("[kill_random] Kill thread {rand_tid} (unix_tid: {unix_tid})");
                unsafe {
                    let _ = libc::syscall(libc::SYS_tgkill, pid, unix_tid, SIGUSR2);
                };
                return;
            }
        }
    }

    /// child thread handler: self panic
    #[cfg(feature = "simulate_tcrash")]
    pub fn self_panic(_signum: usize) {
        println!("[self_panic] {}", unsafe { gettid() });
        panic!("[self_panic] {}", unsafe { gettid() });
    }
}
