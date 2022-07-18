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
                    new.with_tid(0),
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
                    new.with_tid(0),
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
                    new.with_tid(0),
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
                old.with_tid(0),
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

mod counter {
    #![allow(unreachable_pub)]
    #![allow(dead_code)]

    use std::sync::atomic::Ordering;

    use crossbeam_epoch::{unprotected, Guard};

    use crate::{
        pepoch::{atomic::Pointer, PAtomic, PShared},
        ploc::Checkpoint,
        pmem::{Collectable, GarbageCollection, PoolHandle},
        PDefault,
    };

    use super::{Cas, DetectableCASAtomic};

    #[derive(Default)]
    pub struct Increment {
        old_new: Checkpoint<(PAtomic<usize>, PAtomic<usize>)>,
        cas: Cas,
    }

    impl Collectable for Increment {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Collectable::filter(&mut s.old_new, tid, gc, pool);
            Collectable::filter(&mut s.cas, tid, gc, pool);
        }
    }

    impl PDefault for Increment {
        fn pdefault(_: &PoolHandle) -> Self {
            Default::default()
        }
    }

    #[derive(Debug, Default)]
    pub struct Counter {
        cnt: DetectableCASAtomic<usize>,
    }

    impl Collectable for Counter {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Collectable::filter(&mut s.cnt, tid, gc, pool);
        }
    }

    impl PDefault for Counter {
        fn pdefault(pool: &PoolHandle) -> Self {
            let s = Self::default();
            assert_eq!(s.peek(unsafe { unprotected() }, pool), 0);
            s
        }
    }

    impl Counter {
        pub fn increment<const REC: bool>(
            &self,
            inc: &mut Increment,
            tid: usize,
            guard: &Guard,
            pool: &PoolHandle,
        ) {
            if self.try_increment::<REC>(inc, tid, guard, pool).is_ok() {
                return;
            }

            loop {
                if self.try_increment::<false>(inc, tid, guard, pool).is_ok() {
                    return;
                }
            }
        }

        fn try_increment<const REC: bool>(
            &self,
            inc: &mut Increment,
            tid: usize,
            guard: &Guard,
            pool: &PoolHandle,
        ) -> Result<(), ()> {
            let (old, new) = inc.old_new.checkpoint::<REC, _>(
                || {
                    let old = self.peek(guard, pool);
                    let new = unsafe { PShared::from_usize(old + 1) };
                    (
                        PAtomic::from(unsafe { PShared::from_usize(old) }),
                        PAtomic::from(new),
                    )
                },
                tid,
                pool,
            );
            let (old, new) = (
                old.load(Ordering::Relaxed, guard),
                new.load(Ordering::Relaxed, guard),
            );

            self.cnt
                .cas::<REC>(old, new, &mut inc.cas, tid, guard, pool)
                .map_err(|_| ())
        }

        #[inline]
        pub fn peek(&self, guard: &Guard, pool: &PoolHandle) -> usize {
            self.cnt.load(Ordering::SeqCst, guard, pool).into_usize()
        }
    }
}

#[cfg(test)]
mod test {
    use super::{
        counter::{Counter, Increment},
        *,
    };
    use crate::{
        pmem::{ralloc::Collectable, RootObj},
        test_utils::tests::*,
    };

    const NR_THREAD: usize = 12;
    const COUNT: usize = 10_000;

    struct Increments {
        increments: [Increment; COUNT],
    }

    impl Default for Increments {
        fn default() -> Self {
            Self {
                increments: array_init::array_init(|_| Default::default()),
            }
        }
    }

    impl Collectable for Increments {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..COUNT {
                Increment::filter(&mut m.increments[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<DetectableCas> for TestRootObj<DetectableCASAtomic<usize>> {
        fn run(
            &self,
            _cas_test: &mut DetectableCas,
            tid: usize,
            _guard: &Guard,
            _pool: &PoolHandle,
        ) {
            match tid {
                // T1: Check the execution results of other threads
                1 => {
                    // Wait for all other threads to finish
                    // while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // // Check results
                    // // TODO: Use from-old-to-new pair
                    // let mut last_tid = 0;
                    // for t in 2..NR_THREAD + 2 {
                    //     let cnt = RESULTS[t].load(Ordering::SeqCst);
                    //     if cnt == COUNT {
                    //         continue;
                    //     }
                    //     assert_eq!(cnt, COUNT - 1);
                    //     assert_eq!(last_tid, 0);
                    //     last_tid = t;
                    // }
                }
                // Threads other than T1 perform CAS
                _ => {
                    // let new = unsafe { PShared::from_usize(tid) };
                    // for i in 0..COUNT {
                    //     let old = loop {
                    //         let old = self.obj.load(Ordering::SeqCst, guard, pool);
                    //         if self
                    //             .obj
                    //             .cas::<true>(old, new, &mut cas_test.cases[i], tid, guard, pool)
                    //             .is_ok()
                    //         {
                    //             break old;
                    //         }
                    //     };

                    //     // Transfer the old value to the result array
                    //     // TODO: Use from-old-to-new pair
                    //     let _ = RESULTS[old.into_usize()].fetch_add(1, Ordering::SeqCst);
                    // }

                    // let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
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

        run_test::<TestRootObj<Counter>, Increments>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }
}
