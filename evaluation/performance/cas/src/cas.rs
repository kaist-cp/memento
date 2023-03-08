use std::sync::atomic::Ordering;

use crossbeam_epoch::unprotected;
use memento::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    ploc::Handle,
    pmem::{Collectable, GarbageCollection, PoolHandle, RootObj},
    Collectable, Memento, PDefault,
};

use crate::{
    cas_random_loc, Node, PFixedVec, TestNOps, TestableCas, CONTENTION_WIDTH, DURATION, TOTAL_NOPS,
    TOTAL_NOPS_FAILED,
};

#[derive(Collectable)]
pub struct TestCas {
    locs: PFixedVec<PAtomic<Node>>,
}

impl PDefault for TestCas {
    fn pdefault(handle: &Handle) -> Self {
        Self {
            locs: PFixedVec::new(unsafe { CONTENTION_WIDTH }, handle),
        }
    }
}

impl TestNOps for TestCas {}

impl TestableCas for TestCas {
    type Location = PAtomic<Node>;
    type Input = usize; // mmt, tid

    fn cas(&self, tid: Self::Input, loc: &Self::Location, _: &Handle) -> bool {
        cas(loc, tid)
    }
}

#[derive(Default, Debug, Memento, Collectable)]
pub struct TestCasMmt {}

impl RootObj<TestCasMmt> for TestCas {
    fn run(&self, _: &mut TestCasMmt, handle: &Handle) {
        let duration = unsafe { DURATION };
        let locs_ref = self.locs.as_ref(&handle.guard, handle.pool);

        let (ops, failed) = self.test_nops(
            &|tid| cas_random_loc(self, tid, locs_ref, handle),
            handle.tid,
            duration,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        let _ = TOTAL_NOPS_FAILED.fetch_add(failed, Ordering::SeqCst);
    }
}

fn cas(loc: &PAtomic<Node>, tid: usize) -> bool {
    let guard = unsafe { unprotected() };

    let old = loc.load(Ordering::SeqCst, guard);
    let new = unsafe { PShared::from_usize(tid) }; // TODO: various new value
    loc.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard)
        .is_ok()
}
