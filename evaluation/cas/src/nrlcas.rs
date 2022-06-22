use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::{unprotected, Guard};
use evaluation::common::{DURATION, MAX_THREADS, TOTAL_NOPS};
use memento::{
    pepoch::PAtomic,
    pmem::{persist_obj, Collectable, GarbageCollection, PoolHandle, RootObj},
    PDefault,
};

use crate::{TestNOps, TOTAL_NOPS_FAILED};

pub struct TestNRLCas {
    loc: PAtomic<usize>,                              // C: val with tid tag
    loc_r: [[AtomicUsize; MAX_THREADS]; MAX_THREADS], // R: [N][N]  // TODO: loc 하나당 r 하나
}

impl Collectable for TestNRLCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestNRLCas {
    fn pdefault(_: &PoolHandle) -> Self {
        Self {
            loc: Default::default(),
            loc_r: array_init::array_init(|_| array_init::array_init(|_| Default::default())),
        }
    }
}

impl TestNOps for TestNRLCas {}

#[derive(Default, Debug)]
pub struct TestNRLCasMmt {
    value: PAtomic<usize>,
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

impl PDefault for TestNRLCasMmt {
    fn pdefault(pool: &PoolHandle) -> Self {
        Self {
            value: PAtomic::new(0, pool),
        }
    }
}

impl Collectable for TestNRLCasMmt {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl RootObj<TestNRLCasMmt> for TestNRLCas {
    fn run(&self, local: &mut TestNRLCasMmt, tid: usize, _: &Guard, pool: &PoolHandle) {
        let duration = unsafe { DURATION };

        let (ops, failed) = self.test_nops(&|tid| nrl_cas(tid, &self, local, pool), tid, duration);

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        let _ = TOTAL_NOPS_FAILED.fetch_add(failed, Ordering::SeqCst);
    }
}

const NULL: usize = 0;

fn nrl_cas(tid: usize, shared: &TestNRLCas, local: &TestNRLCasMmt, pool: &PoolHandle) -> bool {
    let guard = unsafe { unprotected() };
    let old = nrl_read(shared, guard, pool);
    let new = tid; // TODO: 다양한 new 값
    nrl_cas_inner(old, new, tid, shared, local, guard, pool)
}

fn nrl_cas_inner<'g>(
    old_value: usize,
    new_value: usize,
    tid: usize,
    shared: &TestNRLCas,
    local: &TestNRLCasMmt,
    guard: &Guard,
    pool: &PoolHandle,
) -> bool {
    // Check old
    let old_p = shared.loc.load(Ordering::SeqCst, guard);
    if !old_p.is_null() {
        let (id, val) = (old_p.tid(), unsafe { old_p.deref(pool) });
        if *val != old_value {
            return false;
        }
        if id != NULL {
            shared.loc_r[id][tid].store(*val, Ordering::SeqCst);
            persist_obj(&shared.loc_r[id][tid], true);
        }
    }

    // Make new
    let mut new_p = local.value.load(Ordering::SeqCst, guard); // TODO: seq로 value 구분
    unsafe { *(new_p.deref_mut(pool)) = new_value };
    let new_p = new_p.with_tid(tid);

    // CAS
    let res = shared
        .loc
        .compare_exchange(old_p, new_p, Ordering::SeqCst, Ordering::SeqCst, guard)
        .is_ok();
    persist_obj(&shared.loc, true);
    res
}

fn nrl_read(shared: &TestNRLCas, guard: &Guard, pool: &PoolHandle) -> usize {
    *unsafe { shared.loc.load(Ordering::SeqCst, guard).deref(pool) }
}
