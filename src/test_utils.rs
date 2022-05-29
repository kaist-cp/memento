//! Utilities

#[doc(hidden)]
pub mod tests {
    use crossbeam_epoch::Guard;
    use std::collections::HashSet;
    use std::io::Error;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tempfile::NamedTempFile;

    use crate::pmem::pool::*;
    use crate::pmem::ralloc::{Collectable, GarbageCollection};
    use crate::PDefault;

    #[cfg(feature = "simulate_tcrash")]
    use {
        crate::ploc::NR_MAX_THREADS,
        libc::{gettid, size_t, SIGUSR1, SIGUSR2},
        std::sync::atomic::{AtomicBool, AtomicI32},
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
        pub static ref TEST_FINISHED: AtomicBool = AtomicBool::new(false);
    }

    /// run test op
    pub fn run_test<O, M, P>(pool_name: P, pool_len: usize, nr_memento: usize)
    where
        O: RootObj<M> + Send + Sync,
        M: Collectable + Default + Send + Sync,
        P: AsRef<Path>,
    {
        #[cfg(feature = "simulate_tcrash")]
        {
            println!("Install `kill_random` and `self_panic` handler");
            let _ = unsafe { libc::signal(SIGUSR1, kill_random as size_t) };
            let _ = unsafe { libc::signal(SIGUSR2, self_panic as size_t) };
        }

        // initialze test variables
        JOB_FINISHED.store(0, Ordering::SeqCst);
        for res in RESULTS.as_ref() {
            res.store(0, Ordering::SeqCst);
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
            let rand_tid = rand::random::<usize>() % UNIX_TIDS.len();
            let unix_tid = UNIX_TIDS[rand_tid].load(Ordering::SeqCst);

            if unix_tid > pid {
                println!("[kill_random] Kill thread {rand_tid} (unix_tid: {unix_tid})");
                unsafe {
                    let _ = libc::syscall(libc::SYS_tgkill, pid, unix_tid, SIGUSR2);
                };
                return;
            }

            if TEST_FINISHED.load(Ordering::SeqCst) {
                println!("[kill_random] No one killed. Because test was already finished.");
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
