use std::sync::atomic::Ordering;

use crossbeam_epoch::{unprotected, Guard};
use memento::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    ploc::Handle,
    pmem::{persist_obj, Collectable, GarbageCollection, PoolHandle, RootObj},
    Collectable, Memento, PDefault,
};

use crate::{
    cas_random_loc, pick_range, Node, PFixedVec, TestNOps, TestableCas, CONTENTION_WIDTH, DURATION,
    NR_THREADS, TOTAL_NOPS, TOTAL_NOPS_FAILED,
};

pub struct NRLLoc {
    c: PAtomic<Node>,               // C: location (val with tid tag)
    r: PFixedVec<PFixedVec<usize>>, // R: msgs for location [N][N]
}

impl Collectable for NRLLoc {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl PDefault for NRLLoc {
    fn pdefault(handle: &Handle) -> Self {
        Self {
            c: PAtomic::from(
                unsafe { PShared::from_usize(0) }
                    .with_tid(pick_range(1, unsafe { NR_THREADS } + 1)),
            ),
            r: PFixedVec::new(unsafe { NR_THREADS } + 1, handle),
        }
    }
}

impl PDefault for PFixedVec<usize> {
    fn pdefault(handle: &Handle) -> Self {
        PFixedVec::new(unsafe { NR_THREADS } + 1, handle)
    }
}

pub struct TestNRLCas {
    locs: PFixedVec<NRLLoc>,
}

impl Collectable for TestNRLCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl PDefault for TestNRLCas {
    fn pdefault(handle: &Handle) -> Self {
        Self {
            locs: PFixedVec::new(unsafe { CONTENTION_WIDTH }, handle),
        }
    }
}

impl TestNOps for TestNRLCas {}

impl TestableCas for TestNRLCas {
    type Input = ();
    type Location = NRLLoc;

    fn cas(&self, _: Self::Input, loc: &Self::Location, handle: &Handle) -> bool {
        nrl_cas(loc, handle.tid, handle.pool)
    }
}

#[derive(Default, Debug, Memento, Collectable)]
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

impl RootObj<TestNRLCasMmt> for TestNRLCas {
    fn run(&self, _: &mut TestNRLCasMmt, handle: &Handle) {
        let duration = unsafe { DURATION };
        let locs_ref = self.locs.as_ref(&handle.guard, handle.pool);

        let (ops, failed) = self.test_nops(
            &|_| cas_random_loc(self, (), locs_ref, handle),
            handle.tid,
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
