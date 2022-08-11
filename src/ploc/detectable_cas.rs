//! General SMO

use std::sync::atomic::{compiler_fence, AtomicU64, Ordering};

use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;
use std::ops::Deref;

use crate::{
    impl_left_bits,
    pepoch::{PAtomic, PShared},
    pmem::{ll::persist_obj, sfence, Collectable, GarbageCollection, PoolHandle},
    PDefault,
};

use super::{ExecInfo, Timestamp, NR_MAX_THREADS};

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
    inner: [CachePadded<AtomicU64>; 2],
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
    fn store(&self, parity: bool, t: Timestamp) {
        self.inner[parity as usize].store(t.into(), Ordering::SeqCst);
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
            .map(|_| persist_obj(&self.inner[parity as usize], false))
            .map_err(|_| ())
    }

    #[inline]
    fn max(&self) -> Timestamp {
        let f = Timestamp::from(self.inner[0].load(Ordering::SeqCst));
        let t = Timestamp::from(self.inner[1].load(Ordering::SeqCst));

        std::cmp::max(f, t)
    }
}

/// Detectable CAS Atomic pointer
#[derive(Debug)]
pub struct DetectableCASAtomic<N: Collectable> {
    /// Atomic pointer
    pub inner: PAtomic<N>, // TODO: CachePadded
}

impl<N: Collectable> Collectable for DetectableCASAtomic<N> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.inner, tid, gc, pool);
    }
}

impl<N: Collectable> Default for DetectableCASAtomic<N> {
    fn default() -> Self {
        Self {
            inner: PAtomic::null(),
        }
    }
}

impl<N: Collectable> PDefault for DetectableCASAtomic<N> {
    fn pdefault(_: &PoolHandle) -> Self {
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
        &self,
        old: PShared<'_, N>,
        new: PShared<'_, N>,
        mmt: &mut Cas,
        tid: usize,
        guard: &'g Guard,
        pool: &PoolHandle,
        rec: &mut bool,
    ) -> Result<(), PShared<'g, N>> {
        if *rec {
            if let Some(ret) = self.cas_result(new, mmt, tid, &pool.exec_info, guard) {
                return ret;
            }
            *rec = false;
        }

        let (p_own, _, _) = pool.exec_info.cas_info.own[tid].load().decode();
        let tmp_new = new.with_aux_bit((!p_own) as _).with_tid(tid);

        loop {
            let res = self.inner.compare_exchange(
                old,
                tmp_new,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );

            if let Err(e) = res {
                let cur = self.load_help(e.current, &pool.exec_info, guard);
                if cur == old {
                    // retry for the property of strong CAS
                    continue;
                }

                mmt.checkpoint_fail(tid, &pool.exec_info);
                return Err(cur);
            }

            // If successful, persist the location
            persist_obj(&self.inner, true);

            // Checkpoint success
            mmt.checkpoint_succ(!p_own, tid, &pool.exec_info);

            // By inserting a pointer with tid removed, it prevents further helping.
            let _ = self
                .inner
                .compare_exchange(
                    tmp_new,
                    new.with_aux_bit(0).with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                )
                .map_err(|_| sfence()); // In case of CAS failure, sfence is required for synchronous flush.

            return Ok(());
        }
    }

    #[inline]
    fn cas_result<'g>(
        &self,
        new: PShared<'_, N>,
        mmt: &mut Cas,
        tid: usize,
        exec_info: &ExecInfo,
        guard: &'g Guard,
    ) -> Option<Result<(), PShared<'g, N>>> {
        let (_, f_mmt, t_mmt) = mmt.checkpoint.decode();
        let t_local = exec_info.local_max_time.load(tid);

        let (p_own, _, t_own) = exec_info.cas_info.own[tid].load().decode();

        if t_mmt > t_local {
            if f_mmt {
                // failed
                exec_info.local_max_time.store(tid, t_mmt);
                let cur = self.inner.load(Ordering::SeqCst, guard);
                let cur = self.load_help(cur, exec_info, guard);
                return Some(Err(cur));
            }

            // already successful
            if t_mmt >= t_own {
                exec_info.cas_info.own[tid].store(mmt.checkpoint);
                let _ = self.inner.compare_exchange(
                    new.with_aux_bit(p_own as _).with_tid(tid),
                    new.with_aux_bit(0).with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            }

            exec_info.local_max_time.store(tid, t_mmt);
            return Some(Ok(()));
        }

        let cur = self.inner.load(Ordering::SeqCst, guard);

        // Check if the CAS I did before crash remains as it is
        if cur.with_aux_bit(0) == new.with_aux_bit(0).with_tid(tid) {
            persist_obj(&self.inner, true);
            mmt.checkpoint_succ(!p_own, tid, exec_info);

            let _ = self
                .inner
                .compare_exchange(
                    cur,
                    new.with_aux_bit(0).with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                )
                .map_err(|_| sfence());

            return Some(Ok(()));
        }

        let t_help = exec_info.cas_info.help[tid].load(!p_own);
        if t_own >= t_help {
            return None;
        }

        // Success because the checkpoint written by the helper is higher than the last CAS
        // Since the value of location has already been changed, I just need to finalize my checkpoint.
        mmt.checkpoint_succ(!p_own, tid, exec_info);
        sfence();

        Some(Ok(()))
    }

    /// Load
    #[inline]
    pub fn load<'g>(&self, ord: Ordering, guard: &'g Guard, pool: &PoolHandle) -> PShared<'g, N> {
        let cur = self.inner.load(ord, guard);
        self.load_help(cur, &pool.exec_info, guard)
    }

    const PATIENCE: u64 = 40000;

    #[inline]
    fn load_help<'g>(
        &self,
        mut old: PShared<'g, N>,
        exec_info: &ExecInfo,
        guard: &'g Guard,
    ) -> PShared<'g, N> {
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
                    if wait2 > t_cur + exec_info.tsc_offset {
                        // TODO: use PATIENCE
                        break 'chk t_cur;
                    }
                }
            };

            let winner_tid = old.tid();
            let winner_parity = old.aux_bit() != 0;

            // check if winner thread's help timestamp is stale
            let t_help = exec_info.cas_info.help[winner_tid].load(winner_parity);
            if t_cur <= t_help {
                // Someone may already help it. I should retry to load.
                old = self.inner.load(Ordering::SeqCst, guard);
                continue;
            }

            // persist the pointer before CASing winner thread's help timestamp
            persist_obj(&self.inner, false);

            // CAS winner thread's pcheckpoint
            if exec_info.cas_info.help[winner_tid]
                .compare_exchange(winner_parity, t_help, t_cur)
                .is_err()
            {
                // Someone may already help it. I should retry to load.
                old = self.inner.load(Ordering::SeqCst, guard);
                continue;
            }

            // help pointer to be clean.
            match self.inner.compare_exchange(
                old,
                old.with_aux_bit(0).with_tid(0),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            ) {
                Ok(ret) => {
                    return ret;
                }
                Err(e) => {
                    old = e.current;
                }
            }
        }
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
pub(crate) struct CasInfo {
    /// Per-thread CAS self-successful time
    pub(crate) own: [CasOwn; NR_MAX_THREADS + 1],

    /// Per-thread Last time receiving CAS helping
    pub(crate) help: &'static CasHelpArr,
}

impl CasInfo {
    pub(crate) fn new(help: &'static CasHelpArr) -> Self {
        Self {
            own: array_init::array_init(|_| CasOwn::default()),
            help,
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
pub struct Cas {
    checkpoint: CasTimestamp, // TODO: CachePadded
}

impl Default for Cas {
    fn default() -> Self {
        Self {
            checkpoint: CasTimestamp(0),
        }
    }
}

impl Collectable for Cas {
    fn filter(mmt: &mut Self, tid: usize, _: &mut GarbageCollection, pool: &mut PoolHandle) {
        // Among CAS clients, those with max checkpoint are recorded
        let (_, f_mmt, t_mmt) = mmt.checkpoint.decode();
        if f_mmt {
            return;
        }

        let (_, _, t_own) = pool.exec_info.cas_info.own[tid].load().decode();

        if t_mmt > t_own {
            pool.exec_info.cas_info.own[tid].store(mmt.checkpoint);
        }
    }
}

impl Cas {
    #[inline]
    fn checkpoint_succ(&mut self, parity: bool, tid: usize, exec_info: &ExecInfo) {
        let t = exec_info.exec_time();
        let ts_succ = CasTimestamp::new(parity, false, t);

        self.checkpoint = ts_succ;
        persist_obj(&self.checkpoint, false); // CAS soon

        compiler_fence(Ordering::Release);

        exec_info.cas_info.own[tid].store(ts_succ);
        exec_info.cas_info.help[tid].store(!parity, t); // preventing other threads from helping the previous CAS.
        exec_info.local_max_time.store(tid, t);
    }

    #[inline]
    fn checkpoint_fail(&mut self, tid: usize, exec_info: &ExecInfo) {
        let t = exec_info.exec_time();
        let ts_fail = CasTimestamp::new(false, true, t);
        self.checkpoint = ts_fail;
        persist_obj(&self.checkpoint, true);
        exec_info.local_max_time.store(tid, t);
    }

    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.checkpoint = CasTimestamp::new(false, false, Timestamp::from(0));
        persist_obj(&self.checkpoint, false);
    }
}

#[allow(unused)]
#[cfg(test)]
mod test {
    use crate::{
        pepoch::{atomic::Pointer, POwned},
        pmem::{persist_obj, ralloc::Collectable, RootObj},
        test_utils::tests::*,
    };

    use std::sync::atomic::Ordering;

    use crossbeam_epoch::Guard;
    use etrace::some_or;

    use crate::{
        pepoch::{PAtomic, PShared},
        ploc::Checkpoint,
        pmem::{GarbageCollection, PoolHandle},
        PDefault,
    };

    use super::{Cas, DetectableCASAtomic};

    #[derive(Debug)]
    pub(crate) struct Node<T: Collectable> {
        pub(crate) data: T,
    }

    impl<T: Collectable> Collectable for Node<T> {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Collectable::filter(&mut s.data, tid, gc, pool);
        }
    }

    #[derive(Debug)]
    pub(crate) struct Swap<T: Collectable> {
        old: Checkpoint<PAtomic<Node<T>>>,
        cas: Cas,
    }

    impl<T: Collectable> Collectable for Swap<T> {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Collectable::filter(&mut s.old, tid, gc, pool);
            Collectable::filter(&mut s.cas, tid, gc, pool);
        }
    }

    impl<T: Collectable> Default for Swap<T> {
        fn default() -> Self {
            Self {
                old: Default::default(),
                cas: Default::default(),
            }
        }
    }

    #[derive(Debug)]
    pub(crate) struct Location<T: Collectable> {
        loc: DetectableCASAtomic<Node<T>>,
    }

    impl<T: Collectable> Collectable for Location<T> {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Collectable::filter(&mut s.loc, tid, gc, pool);
        }
    }

    impl<T: Collectable> Default for Location<T> {
        fn default() -> Self {
            Self {
                loc: Default::default(),
            }
        }
    }

    impl<T: Collectable> PDefault for Location<T> {
        fn pdefault(_: &PoolHandle) -> Self {
            Self::default()
        }
    }

    impl<T: Collectable> Location<T> {
        #[inline]
        pub(crate) fn cas_wo_failure<'g>(
            &self,
            mut old: PShared<'g, Node<T>>,
            new: PShared<'_, Node<T>>,
            cas: &mut Cas,
            tid: usize,
            guard: &'g Guard,
            pool: &PoolHandle,
            rec: &mut bool,
        ) {
            while self.loc.cas(old, new, cas, tid, guard, pool, rec).is_err() {}
        }

        pub(crate) fn swap<'g>(
            &self,
            new: PShared<'_, Node<T>>,
            swap: &mut Swap<T>,
            tid: usize,
            guard: &'g Guard,
            pool: &PoolHandle,
            rec: &mut bool,
        ) -> PShared<'g, Node<T>> {
            loop {
                if let Ok(old) = self.try_swap(new, swap, tid, guard, pool, rec) {
                    return old;
                }
            }
        }

        fn try_swap<'g>(
            &self,
            new: PShared<'_, Node<T>>,
            swap: &mut Swap<T>,
            tid: usize,
            guard: &'g Guard,
            pool: &PoolHandle,
            rec: &mut bool,
        ) -> Result<PShared<'g, Node<T>>, ()> {
            let old = swap
                .old
                .checkpoint(
                    || {
                        let old = self.loc.load(Ordering::SeqCst, guard, pool);
                        PAtomic::from(old)
                    },
                    tid,
                    pool,
                    rec,
                )
                .load(Ordering::Relaxed, guard);

            if self
                .loc
                .cas(old, new, &mut swap.cas, tid, guard, pool, rec)
                .is_ok()
            {
                return Ok(old);
            }

            panic!();
        }
    }

    const NR_THREAD: usize = 2;
    const NR_COUNT: usize = 10_000;

    struct Updates {
        nodes: [Checkpoint<PAtomic<Node<TestValue>>>; NR_COUNT],
        upds: [(Cas, Swap<TestValue>); NR_COUNT],
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
        fn run(&self, mmt: &mut Updates, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let mut rec = true; // TODO: generalize
            let testee = unsafe { TESTER.as_ref().unwrap().testee(tid, true) };
            let loc = &self.obj;

            for seq in 0..NR_COUNT {
                let node = mmt.nodes[seq]
                    .checkpoint(
                        || {
                            let node = POwned::new(
                                Node {
                                    data: TestValue::new(tid, seq),
                                },
                                pool,
                            );
                            persist_obj(unsafe { node.deref(pool) }, true);
                            PAtomic::from(node)
                        },
                        tid,
                        pool,
                        &mut rec,
                    )
                    .load(Ordering::Relaxed, guard);

                loc.cas_wo_failure(
                    PShared::null(),
                    node,
                    &mut mmt.upds[seq].0,
                    tid,
                    guard,
                    pool,
                    &mut rec,
                );

                let old = loc.swap(
                    PShared::null(),
                    &mut mmt.upds[seq].1,
                    tid,
                    guard,
                    pool,
                    &mut rec,
                );

                let val = unsafe { std::ptr::read(&old.deref(pool).data) };
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
}
