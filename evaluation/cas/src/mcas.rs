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

use crate::{
    cas_random_loc, Node, PFixedVec, TestNOps, TestableCas, CONTENTION_WIDTH, TOTAL_NOPS_FAILED,
};

pub struct TestMCas {
    locs: PFixedVec<DetectableCASAtomic<Node>>,
}

impl Collectable for TestMCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl PDefault for TestMCas {
    fn pdefault(pool: &PoolHandle) -> Self {
        Self {
            locs: PFixedVec::new(unsafe { CONTENTION_WIDTH }, pool),
        }
    }
}

impl TestNOps for TestMCas {}

impl TestableCas for TestMCas {
    type Location = DetectableCASAtomic<Node>;
    type Input = (&'static mut Cas, usize); // mmt, tid

    fn cas(
        &self,
        (mmt, tid): Self::Input,
        loc: &Self::Location,
        _: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        mcas(loc, mmt, tid, pool)
    }
}

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
        let locs_ref = unsafe { self.locs.as_ref(unprotected(), pool) };

        let (ops, failed) = self.test_nops(
            &|tid| {
                let mmt = unsafe { (&*mmt.cas as *const _ as *mut Cas).as_mut() }.unwrap();
                cas_random_loc(self, (mmt, tid), locs_ref, unsafe { unprotected() }, pool)
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
    let new = unsafe { PShared::from_usize(tid) }; // TODO: 다양한 new 값 // TODO: from_usize(tid)로 넣으면 offset과 tid 태그로 섞여서 생성되는데 이래도 괜찮나?
    loc.cas::<false>(old, new, mmt, tid, guard, pool).is_ok()
}
