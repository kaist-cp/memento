//! SOFT hash table
use super::soft_list::{thread_ini, Insert, Remove, SOFTList};
use crate::pmem::{Collectable, PoolHandle};
use core::hash::{Hash, Hasher};
use crossbeam_epoch::Guard;
use fasthash::Murmur3Hasher;

const BUCKET_NUM: usize = 16777216;

/// per-thread initialization
pub fn hash_thread_ini(tid: usize, pool: &PoolHandle) {
    thread_ini(tid, pool)
}

/// SOFT HashTable
#[derive(Debug)]
pub struct SOFTHashTable<T: Default> {
    table: [SOFTList<T>; BUCKET_NUM],
}

impl<T: Default> Default for SOFTHashTable<T> {
    fn default() -> Self {
        Self {
            table: array_init::array_init(|_| SOFTList::default()),
        }
    }
}

impl<T: 'static + Default + Clone + PartialEq> SOFTHashTable<T> {
    /// insert
    pub fn insert<const REC: bool>(
        &self,
        k: usize,
        item: T,
        client: &mut HashInsert<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        let bucket = self.get_bucket(k);
        bucket.insert::<REC>(k, item, &mut client.insert, tid, guard, pool)
    }

    /// remove
    pub fn remove<const REC: bool>(
        &self,
        k: usize,
        client: &mut HashRemove<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        let bucket = self.get_bucket(k);
        bucket.remove::<REC>(k, &mut client.remove, tid, guard, pool)
    }

    /// contains
    pub fn contains(&self, k: usize) -> bool {
        let bucket = self.get_bucket(k);
        bucket.contains(k)
    }

    fn get_bucket(&self, k: usize) -> &SOFTList<T> {
        let mut hasher = Murmur3Hasher::default();
        k.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.table[hash % BUCKET_NUM]
    }
}

/// HashInsert client
#[derive(Debug, Default)]
pub struct HashInsert<T: Default + 'static> {
    insert: Insert<T>,
}

impl<T: Default> HashInsert<T> {
    /// clear
    pub fn clear(&mut self) {
        self.insert.clear()
    }
}

impl<T: Default> Collectable for HashInsert<T> {
    fn filter(
        s: &mut Self,
        tid: usize,
        gc: &mut crate::pmem::GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Collectable::filter(&mut s.insert, tid, gc, pool);
    }
}

/// HashRemove client
#[derive(Debug, Default)]
pub struct HashRemove<T: Default + 'static> {
    remove: Remove<T>,
}

impl<T: Default + PartialEq + Clone> HashRemove<T> {
    /// clear
    pub fn clear(&mut self) {
        self.remove.clear()
    }
}

impl<T: Default> Collectable for HashRemove<T> {
    fn filter(
        s: &mut Self,
        tid: usize,
        gc: &mut crate::pmem::GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Collectable::filter(&mut s.remove, tid, gc, pool);
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
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
            todo!()
        }
    }

    #[derive(Debug, Default)]
    struct InsertContainRemove {
        insert: HashInsert<usize>,
        remover: HashRemove<usize>,
    }

    impl Collectable for InsertContainRemove {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
            todo!()
        }
    }

    impl RootObj<InsertContainRemove> for TestRootObj<SOFTHashRoot> {
        fn run(&self, m: &mut InsertContainRemove, tid: usize, guard: &Guard, pool: &PoolHandle) {
            // per-thread init
            let barrier = BARRIER.clone();
            hash_thread_ini(tid, pool);
            let _ = barrier.wait();

            // insert, check, remove, check
            let list = &self.obj.hash;
            let insert_cli = &mut m.insert;
            let remove_cli = &mut m.remover;
            for _ in 0..COUNT {
                let (k, v) = (tid, tid);
                assert!(list.insert::<false>(k, v, insert_cli, tid, guard, pool));
                assert!(list.contains(k));
                assert!(list.remove::<false>(k, remove_cli, tid, guard, pool));
                assert!(!list.contains(k));
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
