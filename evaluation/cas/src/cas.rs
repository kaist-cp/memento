use std::sync::atomic::Ordering;

use crossbeam_epoch::{unprotected, Guard};
use evaluation::common::{DURATION, TOTAL_NOPS};
use memento::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    pmem::{Collectable, GarbageCollection, PoolHandle, RootObj},
    PDefault,
};

use crate::{cas_random_loc, Locations, Node, TestNOps, TestableCas, TOTAL_NOPS_FAILED};

pub struct TestCas {
    locs: Locations<PAtomic<Node>>,
}

impl Collectable for TestCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestCas {
    fn pdefault(pool: &PoolHandle) -> Self {
        Self {
            locs: Locations::pdefault(pool),
        }
    }
}

impl TestNOps for TestCas {}

impl TestableCas for TestCas {
    type Location = PAtomic<Node>;
    type Input = usize; // mmt, tid

    fn cas(&self, tid: Self::Input, loc: &Self::Location, _: &Guard, _: &PoolHandle) -> bool {
        cas(loc, tid)
    }
}

#[derive(Default, Debug)]
pub struct TestCasMmt {}

impl Collectable for TestCasMmt {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl RootObj<TestCasMmt> for TestCas {
    fn run(&self, _: &mut TestCasMmt, tid: usize, _: &Guard, pool: &PoolHandle) {
        let duration = unsafe { DURATION };
        let locs_ref = unsafe { self.locs.as_ref(unprotected(), pool) };

        let (ops, failed) = self.test_nops(
            &|tid| cas_random_loc(self, tid, locs_ref, unsafe { unprotected() }, pool),
            tid,
            duration,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        let _ = TOTAL_NOPS_FAILED.fetch_add(failed, Ordering::SeqCst);
    }
}

fn cas(loc: &PAtomic<Node>, tid: usize) -> bool {
    let guard = unsafe { unprotected() };

    let old = loc.load(Ordering::SeqCst, guard);
    let new = unsafe { PShared::from_usize(tid) }; // TODO: 다양한 new 값
    loc.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard)
        .is_ok()
}
