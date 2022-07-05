use core::sync::atomic::Ordering;
use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::ds::clevel::Delete;
use memento::ds::list::*;
use memento::pmem::pool::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::PDefault;

use crate::common::{pick_range, TestNOps, DURATION, INIT_SIZE, KEY_RANGE, PROB, TOTAL_NOPS};

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
    fn pdefault(pool: &PoolHandle) -> Self {
        // let list = List::pdefault(pool);
        // let guard = epoch::pin();

        // let mut ins_init = Insert::default();
        // for i in 0..unsafe { INIT_SIZE } {
        //     let v = pick_range(1, unsafe { KEY_RANGE });

        //     list.insert(key, value, ins_init, 0, guard, pool);
        // }
        // Self { list }
        todo!("need List::pdefault")
    }
}

impl TestNOps for TestMementoList {}

#[derive(Debug)]
pub struct TestMementoInsDelRd<const INS_RT: f64, const DEL_RT: f64, const RD_RT: f64> {
    ins: CachePadded<Insert<usize, usize>>,
    del: CachePadded<Delete<usize, usize>>,
}

impl<const INS_RT: f64, const DEL_RT: f64, const RD_RT: f64> Default
    for TestMementoInsDelRd<INS_RT, DEL_RT, RD_RT>
{
    fn default() -> Self {
        Self {
            ins: CachePadded::new(Default::default()),
            del: CachePadded::new(Default::default()),
        }
    }
}

impl<const INS_RT: f64, const DEL_RT: f64, const RD_RT: f64> Collectable
    for TestMementoInsDelRd<INS_RT, DEL_RT, RD_RT>
{
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<const INS_RT: f64, const DEL_RT: f64, const RD_RT: f64>
    RootObj<TestMementoInsDelRd<INS_RT, DEL_RT, RD_RT>> for TestMementoList
{
    fn run(
        &self,
        mmt: &mut TestMementoInsDelRd<INS_RT, DEL_RT, RD_RT>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let list = &self.list;

        let ops = self.test_nops(
            &|tid, guard| {
                // unwrap CachePadded
                let ins = unsafe { (&*mmt.ins as *const _ as *mut Insert<usize, usize>).as_mut() }
                    .unwrap();
                let del = unsafe { (&*mmt.del as *const _ as *mut Delete<usize, usize>).as_mut() }
                    .unwrap();

                let op = pick_range(1, 100);
                let key = pick_range(1, unsafe { KEY_RANGE });
                if op <= INS_RT * 100 {
                    // TODO: value?
                    list.insert(key, value, ins, tid, guard, pool);
                } else if op <= INS_RT * 100 + DEL_RT * 100 {
                    todo!("delete(key)")
                } else {
                    todo!("read(key)");
                }
            },
            tid,
            unsafe { DURATION },
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
    }
}
