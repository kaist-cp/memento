use std::sync::atomic::Ordering;

use crossbeam_epoch::unprotected;
use crossbeam_utils::CachePadded;
use memento::{
    pepoch::{atomic::Pointer, PShared},
    ploc::{Cas, DetectableCASAtomic, Handle},
    pmem::{Collectable, GarbageCollection, PoolHandle, RootObj},
    Collectable, Memento, PDefault,
};

use crate::{
    cas_random_loc, Node, PFixedVec, TestNOps, TestableCas, CONTENTION_WIDTH, DURATION, TOTAL_NOPS,
    TOTAL_NOPS_FAILED,
};

pub struct TestMCas {
    locs: PFixedVec<DetectableCASAtomic<Node>>,
}

impl Collectable for TestMCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl PDefault for TestMCas {
    fn pdefault(handle: &Handle) -> Self {
        Self {
            locs: PFixedVec::new(unsafe { CONTENTION_WIDTH }, handle),
        }
    }
}

impl TestNOps for TestMCas {}

impl TestableCas for TestMCas {
    type Location = DetectableCASAtomic<Node>;
    type Input = &'static mut Cas<Node>; // mmt

    fn cas(&self, mmt: Self::Input, loc: &Self::Location, handle: &Handle) -> bool {
        mcas(loc, mmt, handle)
    }
}

#[derive(Default, Debug, Memento, Collectable)]
pub struct TestMCasMmt {
    pub cas: CachePadded<Cas<Node>>,
}

impl RootObj<TestMCasMmt> for TestMCas {
    fn run(&self, mmt: &mut TestMCasMmt, handle: &Handle) {
        let duration = unsafe { DURATION };
        let locs_ref = unsafe { self.locs.as_ref(unprotected(), handle.pool) };

        let (ops, failed) = self.test_nops(
            &|_| {
                let mmt = unsafe { (&*mmt.cas as *const _ as *mut Cas<Node>).as_mut() }.unwrap();
                cas_random_loc(self, mmt, locs_ref, handle)
            },
            handle.tid,
            duration,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        let _ = TOTAL_NOPS_FAILED.fetch_add(failed, Ordering::SeqCst);
    }
}

fn mcas(loc: &DetectableCASAtomic<Node>, mmt: &mut Cas<Node>, handle: &Handle) -> bool {
    let old = loc.load(Ordering::SeqCst, handle);
    let new = unsafe { PShared::from_usize(handle.tid) }; // TODO: various new value
    loc.cas(old, new, mmt, handle).is_ok()
}
