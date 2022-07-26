//! General SMO

use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;

use crate::{
    pepoch::{PAtomic, PShared},
    pmem::{lfence, ll::persist_obj, sfence, Collectable, GarbageCollection, PoolHandle},
    PDefault,
};

use super::{ExecInfo, Timestamp, NR_MAX_THREADS};

pub(crate) type CASCheckpointArr = [CachePadded<AtomicU64>; NR_MAX_THREADS + 1];

/// Detectable CAS Atomic pointer
#[derive(Debug)]
pub struct DetectableCASAtomic<N: Collectable> {
    /// Atomic pointer
    pub inner: PAtomic<N>,
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
    pub fn cas<'g, const REC: bool>(
        &self,
        old: PShared<'_, N>,
        new: PShared<'_, N>,
        mmt: &mut Cas,
        tid: usize,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<(), PShared<'g, N>> {
        if REC {
            if let Some(ret) = self.cas_result(new, mmt, tid, &pool.exec_info, guard) {
                return ret;
            }
        }

        let prev_chk = Timestamp::from(pool.exec_info.cas_info.own[tid].load(Ordering::Relaxed));
        let parity = !Cas::parity_from_timestamp(prev_chk);
        let tmp_new = new.with_aux_bit(Cas::parity_to_bit(parity)).with_tid(tid);

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
            mmt.checkpoint_succ(parity, tid, &pool.exec_info);
            lfence();

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
        let cas_state = mmt.state(tid, exec_info);

        if let CasState::Failure = cas_state {
            exec_info.local_max_time[tid].store(mmt.checkpoint.into(), Ordering::Relaxed);
            let cur = self.inner.load(Ordering::SeqCst, guard);
            return Some(Err(self.load_help(cur, exec_info, guard)));
        }

        let vchk = Timestamp::from(exec_info.cas_info.own[tid].load(Ordering::Relaxed));
        let vchk_par = Cas::parity_from_timestamp(vchk);

        if let CasState::Success = cas_state {
            if mmt.checkpoint > vchk {
                exec_info.cas_info.own[tid].store(mmt.checkpoint.into(), Ordering::Relaxed);
            }

            if mmt.checkpoint >= vchk {
                let _ = self.inner.compare_exchange(
                    new.with_aux_bit(Cas::parity_to_bit(vchk_par)).with_tid(tid),
                    new.with_aux_bit(0).with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            }

            exec_info.local_max_time[tid].store(mmt.checkpoint.into(), Ordering::Relaxed);
            return Some(Ok(()));
        }

        let cur = self.inner.load(Ordering::SeqCst, guard);
        let next_par = !vchk_par;

        // Check if the CAS I did before crash remains as it is
        if cur == new.with_aux_bit(Cas::parity_to_bit(next_par)).with_tid(tid) {
            persist_obj(&self.inner, true);
            mmt.checkpoint_succ(next_par, tid, exec_info);
            let _ = self
                .inner
                .compare_exchange(
                    cur,
                    new.with_aux_bit(0).with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                )
                .map_err(|_| sfence);
            return Some(Ok(()));
        }

        // After successful CAS, check if I received help
        let pchk = Timestamp::from(
            exec_info.cas_info.help[Cas::parity_to_bit(next_par)][tid].load(Ordering::SeqCst),
        );
        if vchk >= pchk {
            return None;
        }

        // Success because the checkpoint written by the helper is higher than the last CAS
        // Since the value of location has already been changed, I just need to finalize my checkpoint.
        mmt.checkpoint_succ(next_par, tid, exec_info);
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

            let chk = 'chk: loop {
                // get checkpoint timestamp
                let start = {
                    let fst = exec_info.exec_time();
                    loop {
                        let snd = exec_info.exec_time();
                        if fst + exec_info.tsc_offset < snd {
                            break snd;
                        }
                    }
                };
                lfence();

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
                    let now = exec_info.exec_time();
                    if now > start + Self::PATIENCE {
                        break 'chk start;
                    }
                }
            };

            let winner_tid = old.tid();
            let winner_bit = old.aux_bit();

            // check if winner thread's pcheckpoint is stale
            let pchk = exec_info.cas_info.help[winner_bit][winner_tid].load(Ordering::SeqCst);
            if chk <= pchk {
                // Someone may already help it. I should retry to load.
                old = self.inner.load(Ordering::SeqCst, guard);
                continue;
            }

            // persist the pointer before CASing winner thread's pcheckpoint
            persist_obj(&self.inner, false);

            // CAS winner thread's pcheckpoint
            if exec_info.cas_info.help[winner_bit][winner_tid]
                .compare_exchange(pchk, chk, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                // Someone may already help it. I should retry to load.
                old = self.inner.load(Ordering::SeqCst, guard);
                continue;
            }

            // help pointer to be clean.
            persist_obj(&exec_info.cas_info.help[winner_bit][winner_tid], false);
            match self.inner.compare_exchange(
                old,
                old.with_aux_bit(0).with_tid(0),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            ) {
                Ok(ret) => return ret,
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
pub(crate) struct CasInfo {
    /// Per-thread CAS self-successful time
    pub(crate) own: CASCheckpointArr,

    /// Per-thread Last time receiving CAS helping
    pub(crate) help: &'static [CASCheckpointArr; 2],
}

impl From<&'static [CASCheckpointArr; 2]> for CasInfo {
    fn from(chk_ref: &'static [CASCheckpointArr; 2]) -> Self {
        Self {
            own: array_init::array_init(|_| CachePadded::new(AtomicU64::new(0))),
            help: chk_ref,
        }
    }
}

/// Compare and Set memento
#[derive(Debug)]
pub struct Cas {
    checkpoint: Timestamp,
}

impl Default for Cas {
    fn default() -> Self {
        Self {
            checkpoint: Timestamp::from(Cas::NOT_CHECKED),
        }
    }
}

impl Collectable for Cas {
    fn filter(cas: &mut Self, tid: usize, _: &mut GarbageCollection, pool: &mut PoolHandle) {
        // Among CAS clients, those with max checkpoint are recorded in vcheckpoint
        let vchk = Timestamp::from(pool.exec_info.cas_info.own[tid].load(Ordering::Relaxed));

        if cas.checkpoint > vchk {
            pool.exec_info.cas_info.own[tid].store(cas.checkpoint.into(), Ordering::Relaxed);
        }
    }
}

enum CasState {
    NotChecked,
    Success,
    Failure,
}

impl Cas {
    #[inline]
    fn checkpoint_succ(&mut self, parity: bool, tid: usize, exec_info: &ExecInfo) {
        let t = exec_info.exec_time();
        let new_chk = Timestamp::new(if parity { Self::PARITY } else { 0 }, t);
        self.checkpoint = new_chk;
        persist_obj(&self.checkpoint, false);
        exec_info.cas_info.own[tid].store(new_chk.into(), Ordering::Relaxed);
        exec_info.local_max_time[tid].store(new_chk.into(), Ordering::Relaxed);
    }

    fn state(&self, tid: usize, exec_info: &ExecInfo) -> CasState {
        if self.checkpoint == Timestamp::from(0)
            || self.checkpoint
                < Timestamp::from(exec_info.local_max_time[tid].load(Ordering::Relaxed))
        {
            return CasState::NotChecked;
        }

        if self.is_failed() {
            return CasState::Failure;
        }

        CasState::Success
    }

    #[inline]
    fn checkpoint_fail(&mut self, tid: usize, exec_info: &ExecInfo) {
        let t = exec_info.exec_time();
        let new_chk = Timestamp::new(Self::FAILED, t);
        self.checkpoint = new_chk;
        persist_obj(&self.checkpoint, true);
        exec_info.local_max_time[tid].store(new_chk.into(), Ordering::Relaxed);
    }

    #[inline]
    fn is_failed(&self) -> bool {
        let tag = self.checkpoint.high_tag();
        tag & Self::FAILED != 0
    }

    const NOT_CHECKED: u64 = 0;
    const FAILED: u64 = 1;
    const PARITY: u64 = 2;

    #[inline]
    fn parity_from_timestamp(t: Timestamp) -> bool {
        let tag = t.high_tag();
        tag & Self::PARITY != 0
    }

    #[inline]
    fn parity_to_bit(p: bool) -> usize {
        if p {
            1
        } else {
            0
        }
    }

    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.checkpoint = Timestamp::from(Cas::NOT_CHECKED);
        persist_obj(&self.checkpoint, false);
    }
}

#[allow(unused)]
#[cfg(test)]
mod test {
    use crate::{
        pmem::{ralloc::Collectable, RootObj},
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
        pub(crate) fn cas_wo_failure<const REC: bool>(
            &self,
            old: PShared<'_, Node<T>>,
            new: PShared<'_, Node<T>>,
            cas: &mut Cas,
            tid: usize,
            guard: &Guard,
            pool: &PoolHandle,
        ) {
            if self.loc.cas::<REC>(old, new, cas, tid, guard, pool).is_ok() {
                return;
            }

            loop {
                if self
                    .loc
                    .cas::<false>(old, new, cas, tid, guard, pool)
                    .is_ok()
                {
                    return;
                }
            }
        }

        pub(crate) fn swap<'g, const REC: bool>(
            &self,
            new: PShared<'_, Node<T>>,
            swap: &mut Swap<T>,
            tid: usize,
            guard: &'g Guard,
            pool: &PoolHandle,
        ) -> PShared<'g, Node<T>> {
            if let Ok(old) = self.try_swap::<REC>(new, swap, tid, guard, pool) {
                return old;
            }

            loop {
                if let Ok(old) = self.try_swap::<false>(new, swap, tid, guard, pool) {
                    return old;
                }
            }
        }

        fn try_swap<'g, const REC: bool>(
            &self,
            new: PShared<'_, Node<T>>,
            swap: &mut Swap<T>,
            tid: usize,
            guard: &'g Guard,
            pool: &PoolHandle,
        ) -> Result<PShared<'g, Node<T>>, ()> {
            let old = swap
                .old
                .checkpoint::<REC, _>(
                    || {
                        let old = self.loc.load(Ordering::SeqCst, guard, pool);
                        PAtomic::from(old)
                    },
                    tid,
                    pool,
                )
                .load(Ordering::Relaxed, guard);

            self.loc
                .cas::<REC>(old, new, &mut swap.cas, tid, guard, pool)
                .map(|_| old)
                .map_err(|_| ())
        }
    }

    const NR_THREAD: usize = 8;
    const NR_COUNT: usize = 100_000;

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
            let testee = unsafe { TESTER.as_ref().unwrap().testee(tid, true) };
            let loc = &self.obj;

            for seq in 0..NR_COUNT {
                let node = mmt.nodes[seq]
                    .checkpoint::<true, _>(
                        || {
                            let node = Node {
                                data: TestValue::new(tid, seq),
                            };
                            PAtomic::new(node, pool)
                        },
                        tid,
                        pool,
                    )
                    .load(Ordering::Relaxed, guard);

                loc.cas_wo_failure::<true>(
                    PShared::null(),
                    node,
                    &mut mmt.upds[seq].0,
                    tid,
                    guard,
                    pool,
                );

                let old = loc.swap::<true>(PShared::null(), &mut mmt.upds[seq].1, tid, guard, pool);
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
