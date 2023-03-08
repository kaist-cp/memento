use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use memento::ds::list::*;
use memento::ploc::Handle;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::pmem::{pool::*, rdtscp};
use memento::{Collectable, Memento, PDefault};

use crate::common::{
    fast_random_range, fast_random_set_seed, TestNOps, DELETE_RATIO, DURATION, INIT_SIZE,
    INSERT_RATIO, KEY_RANGE, TOTAL_NOPS,
};

/// Root obj for evaluation of MementoQueueGeneral
#[derive(Debug, Collectable)]
pub struct TestMementoList {
    list: List<usize, usize>,
}

impl PDefault for TestMementoList {
    fn pdefault(handle: &Handle) -> Self {
        let list = List::pdefault(handle);

        fast_random_set_seed((rdtscp() + 120) as u32);
        let mut ins_init = Insert::default();
        for _ in 0..unsafe { INIT_SIZE } {
            let v = fast_random_range(1, unsafe { KEY_RANGE });
            let _ = list.insert(v, v, &mut ins_init, handle);
        }
        Self { list }
    }
}

impl TestNOps for TestMementoList {}

#[derive(Debug, Memento, Collectable)]
pub struct TestMementoInsDelRd {
    ins: CachePadded<Insert<usize, usize>>,
    del: CachePadded<Delete<usize, usize>>,
}

impl Default for TestMementoInsDelRd {
    fn default() -> Self {
        Self {
            ins: CachePadded::new(Default::default()),
            del: CachePadded::new(Default::default()),
        }
    }
}

impl RootObj<TestMementoInsDelRd> for TestMementoList {
    fn run(&self, mmt: &mut TestMementoInsDelRd, handle: &Handle) {
        let (tid, guard) = (handle.tid, &handle.guard);
        let list = &self.list;

        fast_random_set_seed((rdtscp() + handle.tid as u64) as u32);
        let ops = self.test_nops(
            &|_, _| {
                // unwrap CachePadded
                let ins = unsafe { (&*mmt.ins as *const _ as *mut Insert<usize, usize>).as_mut() }
                    .unwrap();
                let del = unsafe { (&*mmt.del as *const _ as *mut Delete<usize, usize>).as_mut() }
                    .unwrap();

                let op = fast_random_range(1, 100);
                let key = fast_random_range(1, unsafe { KEY_RANGE });
                if op <= unsafe { INSERT_RATIO } {
                    let _ = list.insert(key, key, ins, handle);
                } else if op <= unsafe { INSERT_RATIO } + unsafe { DELETE_RATIO } {
                    let _ = list.delete(&key, del, handle);
                } else {
                    let _ = list.lookup(&key, handle);
                }
            },
            tid,
            unsafe { DURATION },
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
    }
}
