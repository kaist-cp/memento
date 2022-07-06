use core::sync::atomic::Ordering;
use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::ds::list::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::pmem::{global_pool, pool::*, rdtscp};
use memento::PDefault;

use crate::common::{
    fast_random_range, fast_random_set_seed, TestNOps, DELETE_RATIO, DURATION, INIT_SIZE,
    INSERT_RATIO, KEY_RANGE, TOTAL_NOPS,
};

/// Root obj for evaluation of MementoQueueGeneral
#[derive(Debug)]
pub struct TestMementoList {
    list: List<usize, usize>,
}

impl Collectable for TestMementoList {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoList {
    fn pdefault(_: &PoolHandle) -> Self {
        let pool = global_pool().unwrap();
        let list = List::pdefault(pool);
        let guard = epoch::pin();

        fast_random_set_seed((rdtscp() + 120) as u32);
        let mut ins_init = Insert::default();
        for _ in 0..unsafe { INIT_SIZE } {
            let v = fast_random_range(1, unsafe { KEY_RANGE });
            let _ = list.insert::<false>(v, v, &mut ins_init, 0, &guard, pool);
        }
        Self { list }
    }
}

impl TestNOps for TestMementoList {}

#[derive(Debug)]
pub struct TestMementoInsDelRd {
    ins: CachePadded<Insert<usize, usize>>,
    del: CachePadded<Delete<usize, usize>>,
    rd: CachePadded<Lookup<usize, usize>>,
}

impl Default for TestMementoInsDelRd {
    fn default() -> Self {
        Self {
            ins: CachePadded::new(Default::default()),
            del: CachePadded::new(Default::default()),
            rd: CachePadded::new(Default::default()),
        }
    }
}

impl Collectable for TestMementoInsDelRd {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl RootObj<TestMementoInsDelRd> for TestMementoList {
    fn run(&self, mmt: &mut TestMementoInsDelRd, tid: usize, guard: &Guard, _: &PoolHandle) {
        let pool = global_pool().unwrap();
        let list = &self.list;

        fast_random_set_seed((rdtscp() + tid as u64) as u32);
        let ops = self.test_nops(
            &|tid, guard| {
                // unwrap CachePadded
                let ins = unsafe { (&*mmt.ins as *const _ as *mut Insert<usize, usize>).as_mut() }
                    .unwrap();
                let del = unsafe { (&*mmt.del as *const _ as *mut Delete<usize, usize>).as_mut() }
                    .unwrap();
                let rd = unsafe { (&*mmt.rd as *const _ as *mut Lookup<usize, usize>).as_mut() }
                    .unwrap();

                let op = fast_random_range(1, 100);
                let key = fast_random_range(1, unsafe { KEY_RANGE });
                if op <= unsafe { INSERT_RATIO } {
                    let _ = list.insert::<false>(key, key, ins, tid, guard, pool);
                } else if op <= unsafe { INSERT_RATIO } + unsafe { DELETE_RATIO } {
                    let _ = list.delete::<false>(&key, del, tid, guard, pool);
                } else {
                    let _ = list.lookup::<false>(&key, rd, tid, guard, pool);
                }
            },
            tid,
            unsafe { DURATION },
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
    }
}
