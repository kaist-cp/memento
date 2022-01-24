//! TODO doc
use super::soft_list::{thread_ini, Insert, Remove, SOFTList};
use crate::pmem::PoolHandle;
use core::hash::{Hash, Hasher};
use fasthash::Murmur3Hasher;

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

impl<T: Default> Default for SOFTHashTable<T> {
    fn default() -> Self {
        Self {
            table: array_init::array_init(|_| SOFTList::default()),
        }
    }
}

impl<T: 'static + Clone + PartialEq> SOFTHashTable<T> {
    /// TODO: doc
    pub fn insert(&self, k: usize, item: T, client: &mut HashInsert<T>, pool: &PoolHandle) -> bool {
        let bucket = self.get_bucket(k);
        bucket.insert(k, item, &mut client.insert, pool)
    }

    /// TODO: doc
    pub fn remove(&self, k: usize, client: &mut HashRemove<T>, pool: &PoolHandle) -> bool {
        let bucket = self.get_bucket(k);
        bucket.remove(k, &mut client.remove, pool)
    }

    /// TODO: doc
    pub fn contains(&self, k: usize) -> bool {
        let bucket = self.get_bucket(k);
        bucket.contains(k)
    }

    fn get_bucket(&self, k: usize) -> &SOFTList<T> {
        let mut hasher = Murmur3Hasher::default();
        k.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.table[hash % BUCKET_NUM] // TODO: c++에선 abs() 왜함?
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct HashInsert<T> {
    insert: Insert<T>,
}

impl<T> HashInsert<T> {
    /// TODO: doc
    pub fn reset(&mut self) {
        self.insert.reset()
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct HashRemove<T> {
    remove: Remove<T>,
}

impl<T> HashRemove<T> {
    /// TODO: doc
    pub fn reset(&mut self) {
        self.remove.reset()
    }
}

#[cfg(test)]
#[allow(box_pointers)]
mod test {
    use epoch::Guard;
    use lazy_static::*;
    use std::sync::{Arc, Barrier};

    use crate::{
        pmem::{Collectable, GarbageCollection, PoolHandle, RootObj},
        test_utils::tests::{run_test, TestRootObj},
        PDefault,
    };
    use crossbeam_epoch::{self as epoch};

    use super::{hash_thread_ini, HashInsert, HashRemove, SOFTHashTable};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    lazy_static! {
        static ref BARRIER: Arc<Barrier> = Arc::new(Barrier::new(NR_THREAD));
    }

    struct SOFTHashRoot {
        hash: Box<SOFTHashTable<usize>>,
    }

    impl PDefault for SOFTHashRoot {
        #![allow(box_pointers)]
        fn pdefault(_: &PoolHandle) -> Self {
            Self {
                hash: Box::new(SOFTHashTable::default()),
            }
        }
    }

    impl Collectable for SOFTHashRoot {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
            todo!()
        }
    }

    #[derive(Debug, Default)]
    struct InsertContainRemove {
        insert: HashInsert<usize>,
        remover: HashRemove<usize>,
    }

    impl Collectable for InsertContainRemove {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
            todo!()
        }
    }

    impl RootObj<InsertContainRemove> for TestRootObj<SOFTHashRoot> {
        fn run(&self, m: &mut InsertContainRemove, tid: usize, _: &Guard, pool: &PoolHandle) {
            // per-thread init
            let barrier = BARRIER.clone();
            hash_thread_ini(tid, pool);
            let _ = barrier.wait();

            // insert, check, remove, check
            let list = &self.obj.hash;
            let insert_cli = &mut m.insert;
            let remove_cli = &mut m.remover;
            for _ in 0..COUNT {
                assert!(list.insert(tid, tid, insert_cli, pool));
                assert!(list.contains(tid));
                assert!(list.remove(tid, remove_cli, pool));
                assert!(!list.contains(tid));
                insert_cli.reset();
                remove_cli.reset();
            }
        }
    }

    #[test]
    fn insert_contain_remove() {
        const FILE_NAME: &str = "soft_hash_insert_contain_remove.pool";
        const FILE_SIZE: usize = 32 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<SOFTHashRoot>, InsertContainRemove, _>(
            FILE_NAME, FILE_SIZE, NR_THREAD,
        )
    }
}
