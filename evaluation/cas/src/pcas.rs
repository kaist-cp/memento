use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::{unprotected, Guard};
use memento::{
    pepoch::{
        atomic::{CompareExchangeError, Pointer},
        PAtomic, POwned, PShared,
    },
    ploc::Handle,
    pmem::{persist_obj, AsPPtr, Collectable, GarbageCollection, PPtr, PoolHandle, RootObj},
    Collectable, Memento, PDefault,
};

use crate::{
    cas_random_loc, Node, PFixedVec, TestNOps, TestableCas, CONTENTION_WIDTH, DURATION, TOTAL_NOPS,
    TOTAL_NOPS_FAILED,
};

pub struct TestPCas {
    locs: PFixedVec<PAtomic<Node>>,
}

impl Collectable for TestPCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        // TODO(seungmin): derive
    }
}

impl PDefault for TestPCas {
    fn pdefault(handle: &Handle) -> Self {
        Self {
            locs: PFixedVec::new(unsafe { CONTENTION_WIDTH }, handle),
        }
    }
}

impl TestNOps for TestPCas {}

impl TestableCas for TestPCas {
    type Input = ();
    type Location = PAtomic<Node>;

    fn cas(&self, _: Self::Input, loc: &Self::Location, handle: &Handle) -> bool {
        pcas(loc, handle.tid)
    }
}

#[derive(Default, Debug, Memento, Collectable)]
pub struct TestPCasMmt {}

impl RootObj<TestPCasMmt> for TestPCas {
    fn run(&self, _: &mut TestPCasMmt, handle: &Handle) {
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

fn pcas(loc: &PAtomic<Node>, tid: usize) -> bool {
    let guard = unsafe { unprotected() };

    let old = pcas_read(loc, guard);
    let new = unsafe { PShared::from_usize(tid) }; // TODO: various new value
    persistent_cas(loc, old, new, guard).is_ok()
}

const DIRTY_FLAG: usize = 1;
const PMWCAS_FLAG: usize = 2;
const RDCSS_FLAG: usize = 4;

fn pcas_read<'g>(address: &PAtomic<Node>, guard: &'g Guard) -> PShared<'g, Node> {
    let word = address.load(Ordering::SeqCst, guard);
    if word.high_tag() & DIRTY_FLAG != 0 {
        persist(address, word, guard);
    }
    word.with_high_tag(0)
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
        new_value.with_high_tag(DIRTY_FLAG),
        Ordering::SeqCst,
        Ordering::SeqCst,
        guard,
    )
}

fn persist<T>(address: &PAtomic<T>, value: PShared<T>, guard: &Guard) {
    persist_obj(address, true);

    let _ = address.compare_exchange(
        value,
        value.with_high_tag(value.high_tag() & !DIRTY_FLAG),
        Ordering::SeqCst,
        Ordering::SeqCst,
        guard,
    );
}

const UNDECIDED: usize = 0;
const SUCCEEDED: usize = 2;
const FAILED: usize = 4;

#[derive(Debug, Default, Clone)]
struct WordDescriptor {
    address: PPtr<PAtomic<PMwCasDescriptor>>, // Note: `PMwCasDescriptor` is dummy type. `address` can point to one of `Node`, `WordDescriptor`, or `PMwCasDescriptor`.
    old_value: PShared<'static, Node>,
    new_value: PShared<'static, Node>,
    mwcas_descriptor: PShared<'static, PMwCasDescriptor>,
}

#[derive(Debug)]
pub struct PMwCasDescriptor {
    status: AtomicUsize,
    count: usize,
    words: [WordDescriptor; 4],
}

unsafe impl Send for PMwCasDescriptor {}
unsafe impl Sync for PMwCasDescriptor {}

impl Default for PMwCasDescriptor {
    fn default() -> Self {
        Self {
            status: AtomicUsize::new(UNDECIDED),
            count: 0,
            words: [
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            ],
        }
    }
}

pub struct TestPMwCas {
    locs: PFixedVec<PAtomic<Node>>,
}

impl Collectable for TestPMwCas {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        // TODO(seungmin): derive
    }
}

impl PDefault for TestPMwCas {
    fn pdefault(handle: &Handle) -> Self {
        Self {
            locs: PFixedVec::new(unsafe { CONTENTION_WIDTH }, handle),
        }
    }
}

impl TestNOps for TestPMwCas {}

impl TestableCas for TestPMwCas {
    type Location = PAtomic<Node>;
    type Input = ();

    fn cas(&self, _: Self::Input, loc: &Self::Location, handle: &Handle) -> bool {
        pmwcas(loc, handle.tid, handle.pool)
    }
}

#[derive(Default, Debug, Memento, Collectable)]
pub struct TestPMwCasMmt {}

impl RootObj<TestPMwCasMmt> for TestPMwCas {
    fn run(&self, _: &mut TestPMwCasMmt, handle: &Handle) {
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

fn pmwcas(loc: &PAtomic<Node>, tid: usize, pool: &PoolHandle) -> bool {
    // NOTE: pmwcas github benchmark
    // 1. Allocate new descriptor (https://github.com/microsoft/pmwcas/blob/master/src/benchmarks/mwcas_benchmark.cc#L187)
    // 2. Reserve CAS on descriptor (https://github.com/microsoft/pmwcas/blob/master/src/benchmarks/mwcas_benchmark.cc#L190)
    // 3. Run descriptor (https://github.com/microsoft/pmwcas/blob/master/src/benchmarks/mwcas_benchmark.cc#L194)

    let guard = unsafe { unprotected() };
    let old = pmwcas_read(loc, guard, pool);
    let new = unsafe { PShared::<Node>::from_usize(tid) }; // TODO: various new value

    // Allocate new descriptor
    let md = POwned::new(PMwCasDescriptor::default(), pool).into_shared(guard);
    let mut md_ref = unsafe { md.clone().deref_mut(pool) };

    // Add new entry
    md_ref.words[0] = WordDescriptor {
        address: PPtr::from(unsafe { loc.as_pptr(pool) }.into_offset()),
        old_value: old,
        new_value: new,
        mwcas_descriptor: md,
    };

    pmwcas_inner(md_ref, guard, pool)
}

fn pmwcas_inner(md: &PMwCasDescriptor, guard: &Guard, pool: &PoolHandle) -> bool {
    let md_ptr = unsafe { PShared::from(md.as_pptr(pool)) };
    let mut st = SUCCEEDED;

    'out: for w in md.words.iter() {
        if w.mwcas_descriptor.is_null() {
            continue;
        }

        // (in sorted order on md.words.address)
        loop {
            let rval = install_mwcas_descriptor(w, guard, pool);
            let rval_high = rval.high_tag();
            let old = w.old_value;

            if rval.into_usize() == old.into_usize() {
                // Descriptor successfully installed
                continue 'out;
            } else if rval_high & PMWCAS_FLAG != 0 {
                if rval_high & DIRTY_FLAG != 0 {
                    persist(unsafe { w.address.deref(pool) }, rval, guard);
                }
                // Clashed another on-going MwCAS, help it finish
                let rval_addr = unsafe { rval.deref(pool) };
                pmwcas_inner(rval_addr, guard, pool);
                continue;
            } else {
                st = FAILED;
                break 'out;
            }
        }
    }

    // Persist all target words if Phase 1 succeeded
    if st == SUCCEEDED {
        for w in md.words.iter() {
            if w.mwcas_descriptor.is_null() {
                continue;
            }

            persist(
                unsafe { w.address.deref(pool) },
                md_ptr.with_high_tag(PMWCAS_FLAG | DIRTY_FLAG),
                guard,
            );
        }
    }

    // Finalize the MwCAS’s status
    let mut cur_st = match md.status.compare_exchange(
        UNDECIDED,
        st | DIRTY_FLAG,
        Ordering::SeqCst,
        Ordering::SeqCst,
    ) {
        Ok(_) => st | DIRTY_FLAG,
        Err(e) => e,
    };
    if cur_st & DIRTY_FLAG != 0 {
        persist_obj(&md.status, true);
        cur_st &= !DIRTY_FLAG;
        md.status.store(cur_st, Ordering::SeqCst);
    }

    // Install the final values
    for w in md.words.iter() {
        if w.mwcas_descriptor.is_null() {
            continue;
        }

        let v = if md.status.load(Ordering::SeqCst) == SUCCEEDED {
            w.new_value
        } else {
            w.old_value
        };
        let v = unsafe { PShared::from_usize(v.into_usize()) };
        let expected = md_ptr.with_high_tag(PMWCAS_FLAG | DIRTY_FLAG);
        let target = unsafe { w.address.deref(pool) };
        let rval = if let Err(e) = target.compare_exchange(
            expected,
            v.with_high_tag(DIRTY_FLAG),
            Ordering::SeqCst,
            Ordering::SeqCst,
            guard,
        ) {
            e.current
        } else {
            expected
        };
        if rval == md_ptr.with_high_tag(PMWCAS_FLAG) {
            let _ = target.compare_exchange(rval, v, Ordering::SeqCst, Ordering::SeqCst, guard);
        }
        persist(target, v, guard);
    }

    md.status.load(Ordering::SeqCst) == SUCCEEDED
}

fn install_mwcas_descriptor<'g>(
    wd: &WordDescriptor,
    guard: &'g Guard,
    pool: &PoolHandle,
) -> PShared<'g, PMwCasDescriptor> {
    let ptr =
        unsafe { PShared::from_usize(wd.as_pptr(pool).into_offset()) }.with_high_tag(RDCSS_FLAG);
    let val = loop {
        let target = unsafe { wd.address.deref(pool) };
        let expected = unsafe { PShared::from_usize(wd.old_value.into_usize()) };
        let val = if let Err(e) = target.compare_exchange(
            unsafe { PShared::from_usize(wd.old_value.into_usize()) },
            ptr,
            Ordering::SeqCst,
            Ordering::SeqCst,
            guard,
        ) {
            e.current
        } else {
            expected
        };

        if val.high_tag() & RDCSS_FLAG == 0 {
            break val;
        }

        // Hit another RDCSS operation, help it finish
        let cur = unsafe { PShared::from_usize(val.into_usize()).deref(pool) };
        complete_install(cur, guard, pool);
    };

    if val.into_usize() == wd.old_value.into_usize() {
        // # Successfully installed the RDCSS descriptor
        complete_install(wd, guard, pool);
    }

    val
}

fn complete_install(wd: &WordDescriptor, guard: &Guard, pool: &PoolHandle) {
    let ptr = wd.mwcas_descriptor.with_high_tag(PMWCAS_FLAG | DIRTY_FLAG);
    let u = unsafe { wd.mwcas_descriptor.deref(pool) }
        .status
        .load(Ordering::SeqCst)
        == UNDECIDED;

    let target = unsafe { wd.address.deref(pool) };
    let old =
        unsafe { PShared::from_usize(wd.as_pptr(pool).into_offset()) }.with_high_tag(RDCSS_FLAG);
    let new = if u {
        ptr
    } else {
        unsafe { PShared::from_usize(wd.old_value.into_usize()) }
    };
    let _ = target.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard);
}

fn pmwcas_read<'g>(
    address: &PAtomic<Node>,
    guard: &'g Guard,
    pool: &PoolHandle,
) -> PShared<'g, Node> {
    loop {
        let mut v = address.load(Ordering::SeqCst, guard);
        let mut tag = v.high_tag();
        if tag & RDCSS_FLAG != 0 {
            let v_addr = unsafe { PShared::from_usize(v.into_usize()).deref(pool) };
            complete_install(v_addr, guard, pool);
            continue;
        }

        if tag & DIRTY_FLAG != 0 {
            persist(address, v, guard);
            tag &= !DIRTY_FLAG;
            v = v.with_high_tag(tag);
        }

        if tag & PMWCAS_FLAG != 0 {
            let v_addr = unsafe { PShared::from_usize(v.into_usize()).deref(pool) };
            pmwcas_inner(v_addr, guard, pool);
            continue;
        }
        return v;
    }
}
