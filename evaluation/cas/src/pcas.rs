use std::sync::atomic::Ordering;

use crossbeam_epoch::{unprotected, Guard};
use evaluation::common::{DURATION, TOTAL_NOPS};
use memento::{
    pepoch::{
        atomic::{CompareExchangeError, Pointer},
        PAtomic, PShared,
    },
    pmem::{persist_obj, Collectable, GarbageCollection, PoolHandle, RootObj},
    PDefault,
};

use crate::{Node, TestNOps};

pub struct TestPCas {
    loc: PAtomic<Node>,
}

impl Collectable for TestPCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestPCas {
    fn pdefault(_: &PoolHandle) -> Self {
        Self {
            loc: Default::default(),
        }
    }
}

impl TestNOps for TestPCas {}

#[derive(Default, Debug)]
pub struct TestPCasMmt {}

impl Collectable for TestPCasMmt {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl RootObj<TestPCasMmt> for TestPCas {
    fn run(&self, _: &mut TestPCasMmt, tid: usize, _: &Guard, _: &PoolHandle) {
        let duration = unsafe { DURATION };

        let ops = self.test_nops(&|tid| pcas(&self.loc, tid), tid, duration);

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
    }
}

fn pcas(loc: &PAtomic<Node>, tid: usize) -> bool {
    let guard = unsafe { unprotected() };

    let old = loc.load(Ordering::SeqCst, guard);
    let new = unsafe { PShared::from_usize(tid) }; // TODO: 다양한 new 값
    persistent_cas(loc, old, new, guard).is_ok()
}

const DIRTY_FLAG: usize = 1;

fn pcas_read<'g>(address: &PAtomic<Node>, guard: &'g Guard) -> PShared<'g, Node> {
    let word = address.load(Ordering::SeqCst, guard);
    if word.tag() & DIRTY_FLAG != 0 {
        persist(address, word, guard);
    }
    word.with_tag(0)
}

fn persistent_cas<'g>(
    address: &PAtomic<Node>,
    old_value: PShared<Node>,
    new_value: PShared<'g, Node>,
    guard: &'g Guard,
) -> Result<PShared<'g, Node>, CompareExchangeError<'g, Node, PShared<'g, Node>>> {
    let _ = pcas_read(address, guard);
    // Conduct the CAS with dirty bit set on new value
    address.compare_exchange(
        old_value,
        new_value.with_tag(DIRTY_FLAG),
        Ordering::SeqCst,
        Ordering::SeqCst,
        guard,
    )
}

fn persist(address: &PAtomic<Node>, value: PShared<Node>, guard: &Guard) {
    persist_obj(address, true);
    let _ = address.compare_exchange(
        value,
        value.with_tag(0),
        Ordering::SeqCst,
        Ordering::SeqCst,
        guard,
    );
}
