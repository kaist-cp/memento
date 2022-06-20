use std::sync::atomic::Ordering;

use crossbeam_epoch::{unprotected, Guard};
use crossbeam_utils::CachePadded;
use evaluation::common::{DURATION, TOTAL_NOPS};
use memento::{
    pepoch::{atomic::Pointer, PShared},
    ploc::{Cas, DetectableCASAtomic},
    pmem::{Collectable, GarbageCollection, PoolHandle, RootObj},
    PDefault,
};

use crate::{Node, TestNOps, TOTAL_NOPS_FAILED};

pub struct TestMCas {
    loc: DetectableCASAtomic<Node>,
}

impl Collectable for TestMCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl PDefault for TestMCas {
    fn pdefault(_: &PoolHandle) -> Self {
        Self {
            loc: Default::default(),
        }
    }
}

impl TestNOps for TestMCas {}

#[derive(Default, Debug)]
pub struct TestMCasMmt {
    pub cas: CachePadded<Cas>,
}

impl Collectable for TestMCasMmt {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl RootObj<TestMCasMmt> for TestMCas {
    fn run(&self, mmt: &mut TestMCasMmt, tid: usize, _: &Guard, pool: &PoolHandle) {
        let duration = unsafe { DURATION };

        let (ops, failed) = self.test_nops(
            &|tid| {
                let mmt = unsafe { (&*mmt.cas as *const _ as *mut Cas).as_mut() }.unwrap();
                mcas(&self.loc, mmt, tid, pool)
            },
            tid,
            duration,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        let _ = TOTAL_NOPS_FAILED.fetch_add(failed, Ordering::SeqCst);
    }
}

fn mcas(loc: &DetectableCASAtomic<Node>, mmt: &mut Cas, tid: usize, pool: &PoolHandle) -> bool {
    let guard = unsafe { unprotected() };

    let old = loc.load(Ordering::SeqCst, guard, pool);
    let new = unsafe { PShared::from_usize(tid) }; // TODO: 다양한 new 값
    loc.cas::<false>(old, new, mmt, tid, guard, pool).is_ok()
}
