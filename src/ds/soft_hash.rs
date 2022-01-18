//! TODO doc
use crossbeam_epoch::Guard;
use fasthash::Murmur3Hasher;

use crate::{
    pmem::{Collectable, GarbageCollection, PoolHandle},
    PDefault,
};
use core::hash::{Hash, Hasher};

use super::soft_list::{thread_ini, SOFTList};

const BUCKET_NUM: usize = 16777216;

/// per-thread initialization
pub fn hash_thread_ini(tid: usize, pool: &PoolHandle) {
    thread_ini(tid, pool)
}

/// TODO: doc
#[derive(Debug)]
pub struct SOFTHashTable<T> {
    table: [SOFTList<T>; BUCKET_NUM],
}

impl<T: Default> PDefault for SOFTHashTable<T> {
    fn pdefault(pool: &PoolHandle) -> Self {
        Self {
            table: array_init::array_init(|_| SOFTList::pdefault(pool)),
        }
    }
}

impl<T> Collectable for SOFTHashTable<T> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> SOFTHashTable<T> {
    /// TODO: doc
    pub fn insert(&self, k: usize, item: T, guard: &Guard, pool: &PoolHandle) -> bool {
        let bucket = self.get_bucket(k);
        bucket.insert(k, item, guard, pool)
    }

    /// TODO: doc
    pub fn remove(&self, k: usize, guard: &Guard, pool: &PoolHandle) -> bool {
        let bucket = self.get_bucket(k);
        bucket.remove(k, guard, pool)
    }

    /// TODO: doc
    pub fn contains(&self, k: usize, guard: &Guard) -> bool {
        let bucket = self.get_bucket(k);
        bucket.contains(k, guard)
    }

    fn get_bucket(&self, k: usize) -> &SOFTList<T> {
        let mut hasher = Murmur3Hasher::default();
        k.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.table[hash % BUCKET_NUM] // TODO: c++에선 abs() 왜함?
    }

    fn SOFTrecovery() {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use epoch::Guard;
    use lazy_static::*;
    use std::sync::{Arc, Barrier};

    use crate::{
        pmem::{Collectable, GarbageCollection, PoolHandle, RootObj},
        test_utils::tests::{run_test, TestRootObj},
    };
    use crossbeam_epoch::{self as epoch};

    use super::{hash_thread_ini, SOFTHashTable};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    lazy_static! {
        static ref BARRIER: Arc<Barrier> = Arc::new(Barrier::new(NR_THREAD));
    }

    #[derive(Debug, Default)]
    struct Smoke {}

    impl Collectable for Smoke {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl RootObj<Smoke> for TestRootObj<SOFTHashTable<usize>> {
        fn run(&self, mmt: &mut Smoke, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let list = &self.obj;

            // per-thread init
            let barrier = BARRIER.clone();
            hash_thread_ini(tid, pool);
            let _ = barrier.wait();

            // insert, check
            let guard = epoch::pin();
            for i in 0..COUNT {
                let _ = list.insert(i, tid, &guard, pool);
                let _ = list.insert(i + COUNT, tid, &guard, pool);
                assert!(list.contains(i, &guard));
                assert!(list.contains(i + COUNT, &guard));
            }
        }
    }

    #[test]
    fn insert_contain() {
        const FILE_NAME: &str = "soft_hash_smoke.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<SOFTHashTable<usize>>, InsertContainRemove, _>(
            FILE_NAME, FILE_SIZE, NR_THREAD,
        )
    }

    #[derive(Debug, Default)]
    struct InsertContainRemove {}

    impl Collectable for InsertContainRemove {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl RootObj<InsertContainRemove> for TestRootObj<SOFTHashTable<usize>> {
        fn run(&self, mmt: &mut InsertContainRemove, tid: usize, guard: &Guard, pool: &PoolHandle) {
            // per-thread init
            let barrier = BARRIER.clone();
            hash_thread_ini(tid, pool);
            let _ = barrier.wait();

            // insert, check, remove, check
            let list = &self.obj;
            for _ in 0..COUNT {
                assert!(list.insert(tid, tid, guard, pool));
                assert!(list.contains(tid, guard));
                assert!(list.remove(tid, guard, pool));
                assert!(!list.contains(tid, guard));
            }
        }
    }

    #[test]
    fn insert_contain_remove() {
        const FILE_NAME: &str = "soft_hash_insert_contain_remove.pool";
        const FILE_SIZE: usize = 32 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<SOFTHashTable<usize>>, InsertContainRemove, _>(
            FILE_NAME, FILE_SIZE, NR_THREAD,
        )
    }
}
