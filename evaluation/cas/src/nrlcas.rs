use std::sync::atomic::Ordering;

use crossbeam_epoch::{unprotected, Guard};
use evaluation::common::{DURATION, TOTAL_NOPS};
use memento::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    pmem::{persist_obj, Collectable, GarbageCollection, PoolHandle, RootObj},
    PDefault,
};

use crate::{
    cas_random_loc, pick_range, Node, PFixedVec, TestNOps, TestableCas, CONTENTION_WIDTH,
    NR_THREADS, TOTAL_NOPS_FAILED,
};

pub struct NRLLoc {
    c: PAtomic<Node>,               // C: location (val with tid tag)
    r: PFixedVec<PFixedVec<usize>>, // R: msgs for location [N][N]
}

impl Collectable for NRLLoc {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl PDefault for NRLLoc {
    fn pdefault(pool: &PoolHandle) -> Self {
        Self {
            c: PAtomic::from(
                unsafe { PShared::from_usize(0) }
                    .with_tid(pick_range(1, unsafe { NR_THREADS } + 1)),
            ),
            r: PFixedVec::new(unsafe { NR_THREADS } + 1, pool),
        }
    }
}

impl PDefault for PFixedVec<usize> {
    fn pdefault(pool: &PoolHandle) -> Self {
        PFixedVec::new(unsafe { NR_THREADS } + 1, pool)
    }
}

pub struct TestNRLCas {
    locs: PFixedVec<NRLLoc>,
}

impl Collectable for TestNRLCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl PDefault for TestNRLCas {
    fn pdefault(pool: &PoolHandle) -> Self {
        Self {
            locs: PFixedVec::new(unsafe { CONTENTION_WIDTH }, pool),
        }
    }
}

impl TestNOps for TestNRLCas {}

impl TestableCas for TestNRLCas {
    type Input = usize; // tid
    type Location = NRLLoc;

    fn cas(&self, tid: Self::Input, loc: &Self::Location, _: &Guard, pool: &PoolHandle) -> bool {
        nrl_cas(loc, tid, pool)
    }
}

#[derive(Default, Debug)]
pub struct TestNRLCasMmt {
    // TODO: Add per-thread sequence number to distinguish each value.
    //
    // - "Our recoverable read-write object algorithm assumes that all values
    // written to the object are distinct. This assumption can be easily
    // satisfied by augmenting each written value with a tuple consisting
    // of the writing process’ ID and a per-process sequence number. In
    // some cases, such as the example of a recoverable counter that we
    // present later, this assumption is satisfied due to object semantics
    // and does not require special treatment."
}

impl Collectable for TestNRLCasMmt {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl RootObj<TestNRLCasMmt> for TestNRLCas {
    fn run(&self, _: &mut TestNRLCasMmt, tid: usize, _: &Guard, pool: &PoolHandle) {
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

const NULL: usize = 0;

fn nrl_cas(loc: &NRLLoc, tid: usize, pool: &PoolHandle) -> bool {
    let guard = unsafe { unprotected() };
    let old = nrl_read(loc, guard);
    let new = tid; // TODO: various new value
    nrl_cas_inner(old, new, loc, tid, guard, pool)
}

fn nrl_cas_inner(
    old: usize,
    new: usize,
    loc: &NRLLoc,
    tid: usize,
    guard: &Guard,
    pool: &PoolHandle,
) -> bool {
    // Check old
    let old_p = loc.c.load(Ordering::SeqCst, guard);
    let (id, val) = decompose(old_p);
    if val != old {
        return false;
    }
    if id != NULL {
        let r = loc.r.as_ref(guard, pool);
        let r_id = unsafe { r[id].assume_init_ref() }.as_mut(guard, pool);
        r_id[tid].write(val);
        persist_obj(&r_id[tid], true);
    }

    // CAS
    let res = loc
        .c
        .compare_exchange(
            old_p,
            compose(tid, new),
            Ordering::SeqCst,
            Ordering::SeqCst,
            guard,
        )
        .is_ok();
    persist_obj(&loc.c, true);
    res
}

fn nrl_read(loc: &NRLLoc, guard: &Guard) -> usize {
    let (_id, value) = decompose(loc.c.load(Ordering::SeqCst, guard));
    value
}

// from (id, value) to ptr
fn compose(tid: usize, value: usize) -> PShared<'static, Node> {
    unsafe { PShared::from_usize(value) }.with_tid(tid) // TODO: distinguish each value using per-thread seq
}

// from ptr to (id, value)
fn decompose(ptr: PShared<Node>) -> (usize, usize) {
    (ptr.tid(), ptr.with_tid(NULL).into_usize())
}
