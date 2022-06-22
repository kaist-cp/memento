use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::{unprotected, Guard};
use evaluation::common::{DURATION, MAX_THREADS, TOTAL_NOPS};
use memento::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    pmem::{persist_obj, Collectable, GarbageCollection, PoolHandle, RootObj},
    PDefault,
};

use crate::{cas_random_loc, Locations, Node, TestNOps, TestableCas, TOTAL_NOPS_FAILED};

pub struct NRLLoc {
    c: PAtomic<Node>, // C: location (val with tid tag)
    r: [[AtomicUsize; MAX_THREADS + 1]; MAX_THREADS + 1], // R: msgs for location ([N][N])
}

impl Default for NRLLoc {
    fn default() -> Self {
        Self {
            c: Default::default(),
            r: array_init::array_init(|_| array_init::array_init(|_| Default::default())),
        }
    }
}

pub struct TestNRLCas {
    locs: Locations<NRLLoc>,
}

impl Collectable for TestNRLCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestNRLCas {
    fn pdefault(pool: &PoolHandle) -> Self {
        Self {
            locs: Locations::pdefault(pool),
        }
    }
}

impl TestNOps for TestNRLCas {}

impl TestableCas for TestNRLCas {
    type Input = usize; // tid
    type Location = NRLLoc;

    fn cas(&self, tid: Self::Input, loc: &Self::Location, _: &Guard, _: &PoolHandle) -> bool {
        nrl_cas(loc, tid)
    }
}

#[derive(Default, Debug)]
pub struct TestNRLCasMmt {
    // TODO: value를 구분하기 위한 per-thread seq num 추가.
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

fn nrl_cas(loc: &NRLLoc, tid: usize) -> bool {
    let guard = unsafe { unprotected() };
    let old = nrl_read(loc, guard);
    let new = tid; // TODO: 다양한 new 값
    nrl_cas_inner(old, new, loc, tid, guard)
}

fn nrl_cas_inner(old: usize, new: usize, loc: &NRLLoc, tid: usize, guard: &Guard) -> bool {
    // Check old
    let old_p = loc.c.load(Ordering::SeqCst, guard);
    let (id, val) = decompose(old_p);
    if val != old {
        return false;
    }
    if id != NULL {
        loc.r[id][tid].store(val, Ordering::SeqCst);
        persist_obj(&loc.r[id][tid], true);
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
    unsafe { PShared::from_usize(value) }.with_tid(tid) // TODO: seq로 value 구분
}

// from ptr to (id, value)
fn decompose(ptr: PShared<Node>) -> (usize, usize) {
    (ptr.tid(), ptr.with_tid(NULL).into_usize())
}
