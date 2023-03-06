//! General SMO

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use atomic::fence;
use cfg_if::cfg_if;
use crossbeam_utils::CachePadded;
use mmt_derive::Collectable;
use std::ops::Deref;

use crate::{
    impl_left_bits,
    pepoch::{
        atomic::{CompareExchangeError, Pointer},
        PAtomic, PShared,
    },
    pmem::{ll::persist_obj, sfence, Collectable, GarbageCollection, PoolHandle},
    Memento, PDefault,
};

use super::{Handle, Timestamp, NR_MAX_THREADS};

#[derive(Debug, Clone, Copy)]
struct CasTimestamp(u64);

impl From<CasTimestamp> for u64 {
    #[inline]
    fn from(ct: CasTimestamp) -> u64 {
        ct.0
    }
}

impl CasTimestamp {
    /// 62-bit timestamp with parity and failure bit
    #[inline]
    fn new(parity: bool, fail: bool, ts: Timestamp) -> Self {
        Self::encode(parity, fail, ts)
    }

    const POS_PARITY_BITS: u32 = 0;
    const NR_PARITY_BITS: u32 = 1;
    impl_left_bits!(
        parity_bits,
        Self::POS_PARITY_BITS,
        Self::NR_PARITY_BITS,
        u64
    );

    const POS_FAIL_BITS: u32 = Self::POS_PARITY_BITS + Self::NR_PARITY_BITS;
    const NR_FAIL_BITS: u32 = 1;
    impl_left_bits!(fail_bits, Self::POS_FAIL_BITS, Self::NR_FAIL_BITS, u64);

    #[inline]
    fn encode(parity: bool, fail: bool, ts: Timestamp) -> Self {
        let p = Self::parity_bits()
            & (parity as u64).rotate_right(Self::POS_PARITY_BITS + Self::NR_PARITY_BITS);
        let f = Self::fail_bits()
            & (fail as u64).rotate_right(Self::POS_FAIL_BITS + Self::NR_FAIL_BITS);
        let t = !Self::parity_bits() & !Self::fail_bits() & u64::from(ts);
        Self(p | f | t)
    }

    #[inline]
    /// Decompose Timestamp into parity and failure flag and timestamp
    fn decode(&self) -> (bool, bool, Timestamp) {
        (
            (self.0 & Self::parity_bits())
                .rotate_left(Self::POS_PARITY_BITS + Self::NR_PARITY_BITS)
                != 0,
            (self.0 & Self::fail_bits()).rotate_left(Self::POS_FAIL_BITS + Self::NR_FAIL_BITS) != 0,
            Timestamp::from(!(Self::parity_bits() | Self::fail_bits()) & self.0),
        )
    }
}

/// Thread-local CAS-Own timestamp storage
#[derive(Debug, Default)]
pub(crate) struct CasOwn(CachePadded<AtomicU64>);

impl CasOwn {
    #[inline]
    fn load(&self) -> CasTimestamp {
        CasTimestamp(self.0.load(Ordering::Relaxed))
    }

    #[inline]
    fn store(&self, t: CasTimestamp) {
        self.0.store(t.into(), Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct CasHelp {
    pub(crate) inner: [CachePadded<AtomicU64>; 2],
}

impl Default for CasHelp {
    fn default() -> Self {
        Self {
            inner: [
                CachePadded::new(AtomicU64::new(0)),
                CachePadded::new(AtomicU64::new(0)),
            ],
        }
    }
}

impl CasHelp {
    #[inline]
    fn load(&self, parity: bool) -> Timestamp {
        Timestamp::from(self.inner[parity as usize].load(Ordering::SeqCst))
    }

    #[inline]
    pub(crate) fn compare_exchange(
        &self,
        parity: bool,
        old: Timestamp,
        new: Timestamp,
    ) -> Result<(), ()> {
        self.inner[parity as usize]
            .compare_exchange(old.into(), new.into(), Ordering::SeqCst, Ordering::SeqCst)
            .map(|_| ())
            .map_err(|_| ())
    }

    #[inline]
    fn max(&self) -> Timestamp {
        let f = Timestamp::from(self.inner[0].load(Ordering::SeqCst));
        let t = Timestamp::from(self.inner[1].load(Ordering::SeqCst));

        std::cmp::max(f, t)
    }
}

#[derive(Debug, Default, Collectable)]
pub(crate) struct CasHelpDescriptor(CachePadded<CasHelpDescriptorInner>);

#[derive(Debug, Default, Collectable)]
struct CasHelpDescriptorInner {
    tmp_new: AtomicUsize,
    seq: AtomicUsize, // sequence of help descriptor
}

/// Detectable CAS Atomic pointer
#[derive(Debug, Collectable)]
pub struct DetectableCASAtomic<N: Collectable> {
    /// Atomic pointer
    pub inner: PAtomic<N>,
}

impl<N: Collectable> Default for DetectableCASAtomic<N> {
    fn default() -> Self {
        Self {
            inner: PAtomic::null(),
        }
    }
}

impl<N: Collectable> PDefault for DetectableCASAtomic<N> {
    fn pdefault(_: &Handle) -> Self {
        Default::default()
    }
}

impl<N: Collectable> From<PShared<'_, N>> for DetectableCASAtomic<N> {
    fn from(node: PShared<'_, N>) -> Self {
        Self {
            inner: PAtomic::from(node),
        }
    }
}

impl<N: Collectable> DetectableCASAtomic<N> {
    /// Compare And Set
    pub fn cas<'g>(
        &'g self,
        old: PShared<'_, N>,
        new: PShared<'_, N>,
        mmt: &mut Cas<N>,
        handle: &'g Handle,
    ) -> Result<(), PShared<'_, N>> {
        let (tid, guard, pool) = (handle.tid, &handle.guard, handle.pool);
        if handle.rec.load(Ordering::Relaxed) {
            if let Some(ret) = self.cas_result(new, mmt, handle) {
                return ret;
            }
            handle.rec.store(false, Ordering::Relaxed);
        }

        let (stale, _) = mmt.stale_latest_idx();
        let (p_own, _, _) = pool.exec_info.cas_info.own[tid].load().decode();
        let tmp_new = new.with_aux_bit((!p_own) as _).with_tid(tid);

        loop {
            // 1. First cas
            let res = self.inner.compare_exchange(
                old,
                tmp_new,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );

            if let Err(e) = res {
                let cur = self.load_help(e.current, handle);
                if cur == old {
                    // retry for the property of strong CAS
                    continue;
                }

                mmt.buf[stale].checkpoint_fail(cur, handle);
                return Err(cur);
            }

            // If successful, persist the location
            persist_obj(&self.inner, true);

            // Checkpoint success
            let t = mmt.buf[stale].checkpoint_succ(!p_own, handle);

            // 2. Second cas
            // By inserting a pointer with tid removed, it prevents further helping.
            if let Err(e) = self.inner.compare_exchange(
                tmp_new,
                new.with_aux_bit(0).with_tid(0),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            ) {
                if e.current.desc_bit() == 1 {
                    // Fianlize the help if there is help descriptor.
                    let _ = self.finalize_help(e.current, t, handle);
                } else {
                    // In case of CAS failure, sfence is required for synchronous flush.
                    sfence()
                };
            }
            return Ok(());
        }
    }

    #[inline]
    fn cas_result<'g>(
        &'g self,
        new: PShared<'_, N>,
        mmt: &mut Cas<N>,
        handle: &'g Handle,
    ) -> Option<Result<(), PShared<'_, N>>> {
        let (tid, guard, exec_info) = (handle.tid, &handle.guard, &handle.pool.exec_info);

        let (stale, latest) = mmt.stale_latest_idx();

        let (_, f_mmt, t_mmt) = mmt.buf[latest].checkpoint.decode();
        let t_local = handle.local_max_time.load();

        let (p_own, _, t_own) = exec_info.cas_info.own[tid].load().decode();

        if t_mmt > t_local {
            if f_mmt {
                // failed
                handle.local_max_time.store(t_mmt);
                let cur = mmt.buf[latest].fail_current.load(Ordering::Relaxed, guard);
                return Some(Err(cur));
            }

            // already successful
            if t_mmt >= t_own {
                handle.pool.exec_info.cas_info.own[tid].store(mmt.buf[latest].checkpoint);
                let _ = self.inner.compare_exchange(
                    new.with_aux_bit(p_own as _).with_tid(tid),
                    new.with_aux_bit(0).with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            }

            handle.local_max_time.store(t_mmt);
            return Some(Ok(()));
        }

        // Finalize the CAS
        let _ = self.load(Ordering::SeqCst, handle);

        let t_help = exec_info.cas_info.help[tid].load(!p_own);
        if t_own >= t_help {
            return None;
        }

        // Success because the checkpoint written by the helper is higher than the last CAS
        // Since the value of location has already been changed, I just need to finalize my checkpoint.
        let _ = mmt.buf[stale].checkpoint_succ(!p_own, handle);
        sfence();

        Some(Ok(()))
    }

    /// Compare And Set (Non-detectable ver.)
    ///
    /// Used when the recovery is not critical (e.g. helping CAS).
    /// WARN: The return value is not stable.
    pub fn cas_non_detectable<'g>(
        &'g self,
        old: PShared<'_, N>,
        new: PShared<'_, N>,
        handle: &'g Handle,
    ) -> Result<(), PShared<'_, N>> {
        let guard = &handle.guard;

        let tmp_new = new.with_aux_bit(1);

        loop {
            // 1. First cas
            let res = self.inner.compare_exchange(
                old,
                tmp_new,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );

            if let Err(e) = res {
                let cur = self.load_help(e.current, handle);
                if cur == old {
                    // retry for the property of strong CAS
                    continue;
                }

                return Err(cur);
            }

            // If successful, persist the location
            persist_obj(&self.inner, true);

            // 2. Second cas
            // By inserting a pointer with tid removed, it prevents further helping.
            let _ = self
                .inner
                .compare_exchange(
                    tmp_new,
                    new.with_aux_bit(0).with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    &handle.guard,
                )
                .map_err(|_| {
                    // In case of CAS failure, sfence is required for synchronous flush.
                    sfence()
                });

            return Ok(());
        }
    }

    #[inline]
    fn is_non_detectable_cas(ptr: PShared<'_, N>) -> bool {
        ptr.aux_bit() == 1 && ptr.tid() == 0
    }

    /// Load
    #[inline]
    pub fn load<'g>(&self, ord: Ordering, handle: &'g Handle) -> PShared<'g, N> {
        let cur = self.inner.load(ord, &handle.guard);
        self.load_help(cur, handle)
    }

    // Location State
    // - [parity: 0 | desc: 0 | tid: 0     | data]: `data` is a ptr and it guarantees to be persisted.
    // - [parity: p | desc: 0 | tid: non-0 | data]: `data` is a ptr that may not be persisted. The `tid` wants a help in the context of parity `p`.
    // - [parity: 1 | desc: 0 | tid: 0     | data]: `data` is a ptr that may not be persisted. Someone wants a help but it doesn't need to be announced. (non-detectable way)
    // - [parity: 0 | desc: 1 | tid: non-0 | data]: `data` is a sequence number of `tid`'s descriptor.
    #[inline]
    fn load_help<'g>(&self, mut old: PShared<'g, N>, handle: &'g Handle) -> PShared<'g, N> {
        let (exec_info, guard) = (&handle.pool.exec_info, &handle.guard);
        loop {
            // return if old is clean
            if old.tid() == 0 {
                return old;
            }

            let t_cur = 'chk: loop {
                // get checkpoint timestamp
                let t_cur = {
                    let wait1 = exec_info.exec_time();
                    loop {
                        let now = exec_info.exec_time();
                        if wait1 + exec_info.tsc_offset < now {
                            break now;
                        }
                    }
                };

                // start spin loop
                loop {
                    let cur = self.inner.load(Ordering::SeqCst, guard);

                    // return if cur is clean. (previous chk timestamp is useless.)
                    if cur.tid() == 0 {
                        return cur;
                    }

                    // if old was changed, new spin loop needs to be started.
                    if old != cur {
                        old = cur;
                        break;
                    }

                    // if patience is over, I have to help it.
                    let wait2 = exec_info.exec_time();

                    cfg_if! {
                        if #[cfg(not(feature = "tcrash"))] {
                            let patience = Timestamp::from(40_000);
                        } else {
                            let patience = handle.pool.exec_info.tsc_offset;
                        }
                    };

                    if wait2 > t_cur + patience {
                        break 'chk t_cur;
                    }
                }
            };
            // Persist the pointer before registering help descriptor
            persist_obj(&self.inner, false);

            // If non-detectable CAS is proceeded, just CAS to clean ptr w/o help announcement.
            if Self::is_non_detectable_cas(old) {
                match self.inner.compare_exchange(
                    old,
                    old.with_aux_bit(0).with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                ) {
                    Ok(clean_ptr) => return clean_ptr,
                    Err(e) => old = e.current,
                };

                continue;
            }

            // Register my help descriptor if there is no descriptor yet.
            if old.desc_bit() == 0 {
                #[cfg(feature = "pmcheck")]
                sfence(); // To pass the false positive of PSan.

                match self.register_help(old, handle) {
                    Ok(desc) => {
                        old = desc;
                    }
                    Err(e) => {
                        old = e.current;
                        if old.desc_bit() == 0 {
                            continue;
                        }
                    }
                }
            }

            // Finalize the help descriptor.
            match self.finalize_help(old, t_cur, handle) {
                Ok(clean_ptr) => {
                    return clean_ptr;
                }
                Err(e) => {
                    old = e;
                }
            }
        }
    }

    #[inline]
    fn register_help<'g>(
        &self,
        old: PShared<'g, N>,
        handle: &'g Handle,
    ) -> Result<PShared<'g, N>, CompareExchangeError<'g, N, PShared<'g, N>>> {
        assert!(old.desc_bit() == 0);
        let (my_tid, my_help_desc) = (
            handle.tid,
            &*handle.pool.exec_info.cas_info.help_desc[handle.tid].0,
        );

        // 1. Set my descriptor
        let my_new_seq = my_help_desc.seq.load(Ordering::SeqCst) + 1;
        my_help_desc.seq.store(my_new_seq, Ordering::SeqCst);
        // TODO: Add fence here after relaxing.
        // persist ordering is guaranteed because it lies in same cache line.
        my_help_desc
            .tmp_new
            .store(old.into_usize(), Ordering::SeqCst);
        persist_obj(my_help_desc, false);

        // 2. Register my descriptor
        // representation: [ aux_bit: 0, desc_bit: 1, tid: help leader, payload: sequence of help leader ]
        self.inner.compare_exchange(
            old,
            unsafe { PShared::from_usize(my_new_seq) }
                .with_aux_bit(0)
                .with_desc_bit(1)
                .with_tid(my_tid),
            Ordering::SeqCst,
            Ordering::SeqCst,
            &handle.guard,
        )
    }

    /// Finalize the help descriptor at time `t_cur`.
    #[inline]
    fn finalize_help<'g>(
        &self,
        old: PShared<'g, N>,
        t_cur: Timestamp,
        handle: &'g Handle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        assert!(old.desc_bit() == 1);
        let (cas_info, guard) = (&handle.pool.exec_info.cas_info, &handle.guard);

        let winner_tmp_new = unsafe {
            PShared::<N>::from_usize(
                cas_info.help_desc[old.tid()] // `old.tid` is leader of help.
                    .0
                    .tmp_new
                    .load(Ordering::SeqCst),
            )
        };

        // Check if the sequence value on the ptr and descriptor are the same.
        let seq = old
            .with_aux_bit(0)
            .with_desc_bit(0)
            .with_tid(0)
            .into_usize();
        if seq != cas_info.help_desc[old.tid()].0.seq.load(Ordering::SeqCst) {
            // This help has already done.
            return Err(self.inner.load(Ordering::SeqCst, &handle.guard));
        }

        let winner_tid = winner_tmp_new.tid();
        let winner_parity = winner_tmp_new.aux_bit() != 0;
        let winner_new = winner_tmp_new.with_aux_bit(0).with_desc_bit(0).with_tid(0);

        // CAS winner thread's pcheckpoint
        let t_help = cas_info.help[winner_tid].load(winner_parity);
        if t_cur <= t_help
            || cas_info.help[winner_tid]
                .compare_exchange(winner_parity, t_help, t_cur)
                .is_err()
        {
            return Err(self.inner.load(Ordering::SeqCst, &handle.guard));
        }
        persist_obj(
            &*cas_info.help[winner_tid].inner[winner_parity as usize],
            false,
        );

        // help pointer to be clean.
        let res =
            self.inner
                .compare_exchange(old, winner_new, Ordering::SeqCst, Ordering::SeqCst, guard);
        persist_obj(&self.inner, true); // persist before return
        res.map_err(|e| e.current)
    }
}

unsafe impl<N: Collectable + Send + Sync> Send for DetectableCASAtomic<N> {}
unsafe impl<N: Collectable> Sync for DetectableCASAtomic<N> {}

#[derive(Debug)]
pub(crate) struct CasHelpArr([CasHelp; NR_MAX_THREADS + 1]);

impl Default for CasHelpArr {
    fn default() -> Self {
        Self(array_init::array_init(|_| CasHelp::default()))
    }
}

impl Deref for CasHelpArr {
    type Target = [CasHelp; NR_MAX_THREADS + 1];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) struct CasHelpDescArr([CasHelpDescriptor; NR_MAX_THREADS + 1]);

impl Default for CasHelpDescArr {
    fn default() -> Self {
        Self(array_init::array_init(|_| Default::default()))
    }
}

impl Deref for CasHelpDescArr {
    type Target = [CasHelpDescriptor; NR_MAX_THREADS + 1];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) struct CasInfo {
    /// Per-thread CAS self-successful time
    pub(crate) own: [CasOwn; NR_MAX_THREADS + 1],

    /// Per-thread Last time receiving CAS helping
    pub(crate) help: &'static CasHelpArr,

    /// Per-thread help descriptor
    pub(crate) help_desc: &'static CasHelpDescArr,
}

impl CasInfo {
    pub(crate) fn new(help: &'static CasHelpArr, help_desc: &'static CasHelpDescArr) -> Self {
        Self {
            own: array_init::array_init(|_| CasOwn::default()),
            help,
            help_desc,
        }
    }

    pub(crate) fn max_ts(&self) -> Timestamp {
        let m = self
            .own
            .iter()
            .map(|own| own.load().decode().2)
            .fold(Timestamp::from(0), std::cmp::max);

        self.help
            .iter()
            .map(|help| help.max())
            .fold(m, std::cmp::max)
    }
}

/// Compare and Set memento
#[derive(Debug)]
pub struct Cas<N: Collectable> {
    buf: [CachePadded<CasInner<N>>; 2],
}

impl<N: Collectable> Default for Cas<N> {
    fn default() -> Self {
        Self {
            buf: [Default::default(), Default::default()],
        }
    }
}

impl<N: Collectable> Memento for Cas<N> {
    #[inline]
    fn clear(&mut self) {
        self.buf[0].clear();
        self.buf[1].clear();
    }
}

impl<N: Collectable> Collectable for Cas<N> {
    fn filter(mmt: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut mmt.buf[0], tid, gc, pool);
        Collectable::filter(&mut mmt.buf[1], tid, gc, pool);
    }
}

impl<N: Collectable> Cas<N> {
    #[inline]
    fn stale_latest_idx(&self) -> (usize, usize) {
        let t0 = self.buf[0].checkpoint.decode().2;
        let t1 = self.buf[1].checkpoint.decode().2;

        if t0 < t1 {
            (0, 1)
        } else {
            (1, 0)
        }
    }
}

#[derive(Debug)]
struct CasInner<N: Collectable> {
    checkpoint: CasTimestamp,
    fail_current: PAtomic<N>,
}

impl<N: Collectable> Memento for CasInner<N> {
    #[inline]
    fn clear(&mut self) {
        self.checkpoint = CasTimestamp::new(false, false, Timestamp::from(0));
        self.fail_current = Default::default();
        persist_obj(self, false);
    }
}

impl<N: Collectable> Default for CasInner<N> {
    fn default() -> Self {
        Self {
            checkpoint: CasTimestamp(0),
            fail_current: Default::default(),
        }
    }
}

impl<N: Collectable> Collectable for CasInner<N> {
    fn filter(mmt: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        // Among CAS clients, those with max checkpoint are recorded
        let (_, f_mmt, t_mmt) = mmt.checkpoint.decode();
        if f_mmt {
            return;
        }

        let (_, _, t_own) = pool.exec_info.cas_info.own[tid].load().decode();

        if t_mmt > t_own {
            pool.exec_info.cas_info.own[tid].store(mmt.checkpoint);
        }

        Collectable::filter(&mut mmt.fail_current, tid, gc, pool);
    }
}

impl<N: Collectable> CasInner<N> {
    #[inline]
    fn checkpoint_succ(&mut self, parity: bool, handle: &Handle) -> Timestamp {
        let t = handle.pool.exec_info.exec_time();
        let ts_succ = CasTimestamp::new(parity, false, t);

        self.checkpoint = ts_succ;
        persist_obj(&self.checkpoint, false); // CAS soon

        handle.pool.exec_info.cas_info.own[handle.tid].store(ts_succ);
        handle.local_max_time.store(t);
        t
    }

    #[inline]
    fn checkpoint_fail(&mut self, current: PShared<'_, N>, handle: &Handle) {
        let t = handle.pool.exec_info.exec_time();
        let ts_fail = CasTimestamp::new(false, true, t);
        self.fail_current.store(current, Ordering::Relaxed);
        fence(Ordering::Release);
        self.checkpoint = ts_fail;
        persist_obj(self, true);
        handle.local_max_time.store(t);
    }
}

/// Test
#[allow(dead_code)]
pub mod test {
    use crate::{
        pepoch::POwned,
        ploc::Handle,
        pmem::{alloc::Collectable, persist_obj, RootObj},
        test_utils::tests::*,
        Memento,
    };

    use std::sync::atomic::Ordering;

    use mmt_derive::Collectable;

    use crate::{
        pepoch::{PAtomic, PShared},
        ploc::Checkpoint,
        pmem::{GarbageCollection, PoolHandle},
        PDefault,
    };

    use super::{Cas, DetectableCASAtomic};

    #[derive(Debug, Collectable)]
    pub(crate) struct Node<T: Collectable> {
        pub(crate) data: T,
    }

    #[derive(Debug, Memento, Collectable)]
    pub(crate) struct Swap<T: Collectable> {
        old: Checkpoint<PAtomic<Node<T>>>,
        cas: Cas<Node<T>>,
    }

    impl<T: Collectable> Default for Swap<T> {
        fn default() -> Self {
            Self {
                old: Default::default(),
                cas: Default::default(),
            }
        }
    }

    #[derive(Debug, Collectable)]
    pub(crate) struct Location<T: Collectable> {
        loc: DetectableCASAtomic<Node<T>>,
    }

    impl<T: Collectable> Default for Location<T> {
        fn default() -> Self {
            Self {
                loc: Default::default(),
            }
        }
    }

    impl<T: Collectable> PDefault for Location<T> {
        fn pdefault(_: &Handle) -> Self {
            Self::default()
        }
    }

    impl<T: Collectable> Location<T> {
        #[inline]
        pub(crate) fn cas_wo_failure(
            &self,
            old: PShared<'_, Node<T>>,
            new: PShared<'_, Node<T>>,
            cas: &mut Cas<Node<T>>,
            handle: &Handle,
        ) {
            while self.loc.cas(old, new, cas, handle).is_err() {}
        }

        pub(crate) fn swap<'g>(
            &self,
            new: PShared<'g, Node<T>>,
            swap: &mut Swap<T>,
            handle: &'g Handle,
        ) -> PShared<'g, Node<T>> {
            loop {
                if let Ok(old) = self.try_swap(new, swap, handle) {
                    return old;
                }
            }
        }

        fn try_swap<'g>(
            &self,
            new: PShared<'g, Node<T>>,
            swap: &mut Swap<T>,
            handle: &'g Handle,
        ) -> Result<PShared<'g, Node<T>>, ()> {
            let old = swap
                .old
                .checkpoint(
                    || {
                        let old = self.loc.load(Ordering::SeqCst, handle);
                        PAtomic::from(old)
                    },
                    handle,
                )
                .load(Ordering::Relaxed, &handle.guard);

            if self.loc.cas(old, new, &mut swap.cas, handle).is_ok() {
                return Ok(old);
            }

            panic!();
        }
    }

    const NR_THREAD: usize = 3;
    const NR_COUNT: usize = 10_000;

    struct Updates {
        nodes: [Checkpoint<PAtomic<Node<TestValue>>>; NR_COUNT],
        upds: [(Cas<Node<TestValue>>, Swap<TestValue>); NR_COUNT],
    }

    impl Memento for Updates {
        fn clear(&mut self) {
            for i in 0..NR_COUNT {
                self.nodes[i].clear();
                self.upds[i].0.clear();
                self.upds[i].1.clear();
            }
        }
    }

    impl Default for Updates {
        fn default() -> Self {
            Self {
                nodes: array_init::array_init(|_| Default::default()),
                upds: array_init::array_init(|_| Default::default()),
            }
        }
    }

    impl Collectable for Updates {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..NR_COUNT {
                Collectable::filter(&mut m.nodes[i], tid, gc, pool);
                Collectable::filter(&mut m.upds[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<Updates> for TestRootObj<Location<TestValue>> {
        #[allow(unused_variables)]
        fn run(&self, mmt: &mut Updates, handle: &Handle) {
            #[cfg(not(feature = "pmcheck"))] // TODO: Remove
            let testee = unsafe { TESTER.as_ref().unwrap().testee(true, handle) };
            let loc = &self.obj;

            for seq in 0..NR_COUNT {
                let node = mmt.nodes[seq]
                    .checkpoint(
                        || {
                            let node = POwned::new(
                                Node {
                                    data: TestValue::new(handle.tid, seq),
                                },
                                handle.pool,
                            );
                            persist_obj(unsafe { node.deref(handle.pool) }, true);
                            PAtomic::from(node)
                        },
                        handle,
                    )
                    .load(Ordering::Relaxed, &handle.guard);

                loc.cas_wo_failure(PShared::null(), node, &mut mmt.upds[seq].0, handle);

                let old = loc.swap(PShared::null(), &mut mmt.upds[seq].1, handle);

                let val = unsafe { std::ptr::read(&old.deref(handle.pool).data) };
                #[cfg(not(feature = "pmcheck"))] // TODO: Remove
                testee.report(seq, val);
            }
        }
    }

    // - We should enlarge stack size for the test (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - You can check gc operation from the second time you open the pool:
    //   - The output statement says COUNT * NR_THREAD + 2 blocks are reachable
    //   - where +2 is a pointer to Root, DetectableCASAtomic
    #[test]
    fn detectable_cas() {
        const FILE_NAME: &str = "detectable_cas";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Location<TestValue>>, Updates>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }

    // TODO: Refactoring
    /// Test detectable cas for pmcheck
    #[cfg(feature = "pmcheck")]
    pub fn dcas() {
        const FILE_NAME: &str = "detectable_cas";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Location<TestValue>>, Updates>(FILE_NAME, FILE_SIZE, NR_THREAD, 10);
    }
}
