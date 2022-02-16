//! General SMO

use std::{
    cell::RefCell,
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
};

use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;

use crate::{
    pepoch::{PAtomic, PShared},
    pmem::{
        lfence, ll::persist_obj, rdtscp, sfence, AsPPtr, Collectable, GarbageCollection, PPtr,
        PoolHandle,
    },
};

use super::{ExecInfo, Timestamp, NR_MAX_THREADS};

const NOT_CHECKED: u64 = 0;
const FAILED: u64 = 1;

pub(crate) type CASCheckpointArr = [CachePadded<AtomicU64>; NR_MAX_THREADS + 1];

thread_local! {
    /// 마지막 실패한 cas의 체크포인트
    static LAST_FAILED_CAS: RefCell<Option<PPtr<Timestamp>>> = RefCell::new(None)
}

/// TODO(doc)
#[derive(Debug)]
pub struct DetectableCASAtomic<N: Collectable> {
    /// TODO: doc
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

impl<N: Collectable> From<PShared<'_, N>> for DetectableCASAtomic<N> {
    fn from(node: PShared<'_, N>) -> Self {
        Self {
            inner: PAtomic::from(node),
        }
    }
}

impl<N: Collectable> DetectableCASAtomic<N> {
    /// TODO(doc)
    pub fn cas<'g, const REC: bool>(
        &self,
        old: PShared<'_, N>,
        new: PShared<'_, N>,
        mmt: &mut Cas<N>,
        tid: usize,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<(), PShared<'g, N>> {
        if REC {
            return self.cas_result(new, mmt, tid, &pool.exec_info, guard);
        }

        LAST_FAILED_CAS.with(|c| {
            let mut failed = c.borrow_mut();
            if let Some(last_chk) = *failed {
                unsafe {
                    if last_chk != mmt.checkpoint.as_pptr(pool) {
                        let last_chk_ref = last_chk.deref_mut(pool);
                        std::ptr::write(last_chk_ref as _, Timestamp::from(FAILED));
                        persist_obj(last_chk_ref as &Timestamp, true);
                    } else {
                        *failed = None;
                    }
                }
            }
        });

        let prev_chk =
            Timestamp::from(pool.exec_info.cas_info.cas_own[tid].load(Ordering::Relaxed));
        let parity = !prev_chk.aux();
        let tmp_new = new
            .with_aux_bit(Timestamp::aux_to_bit(parity))
            .with_tid(tid);

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

                LAST_FAILED_CAS.with(|failed| {
                    *failed.borrow_mut() = Some(unsafe { mmt.checkpoint.as_pptr(pool) });
                });

                return Err(cur);
            }

            // 성공하면 target을 persist
            persist_obj(&self.inner, true);

            // 성공했다고 체크포인팅
            mmt.checkpoint_succ(parity, tid, &pool.exec_info);
            lfence();

            // 그후 tid 뗀 포인터를 넣어줌으로써 checkpoint는 필요 없다고 알림
            let _ = self
                .inner
                .compare_exchange(
                    tmp_new,
                    new.with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                )
                .map_err(|_| sfence()); // cas 실패시 synchronous flush를 위해 sfence 해줘야 함

            return Ok(());
        }
    }

    #[inline]
    fn cas_result<'g>(
        &self,
        new: PShared<'_, N>,
        mmt: &mut Cas<N>,
        tid: usize,
        exec_info: &ExecInfo,
        guard: &'g Guard,
    ) -> Result<(), PShared<'g, N>> {
        if mmt.checkpoint == Timestamp::from(FAILED) {
            let cur = self.inner.load(Ordering::SeqCst, guard);
            return Err(self.load_help(cur, exec_info, guard)); // TODO(opt): RecFail?
        }

        let vchk = Timestamp::from(exec_info.cas_info.cas_own[tid].load(Ordering::Relaxed));

        if mmt.checkpoint != Timestamp::from(NOT_CHECKED)
            && mmt.checkpoint
                > Timestamp::from(exec_info.local_max_time[tid].load(Ordering::Relaxed))
        {
            if mmt.checkpoint > vchk {
                exec_info.cas_info.cas_own[tid].store(mmt.checkpoint.into(), Ordering::Relaxed);
            }

            if mmt.checkpoint >= vchk {
                let _ = self.inner.compare_exchange(
                    new.with_aux_bit(Timestamp::aux_to_bit(vchk.aux()))
                        .with_tid(tid),
                    new.with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            }

            exec_info.local_max_time[tid].store(mmt.checkpoint.into(), Ordering::Relaxed);
            return Ok(());
        }

        let cur = self.inner.load(Ordering::SeqCst, guard);
        let next_par = !vchk.aux();

        // 내가 첫 CAS 성공한 채로 그대로 남아 있는지 확인
        if cur
            == new
                .with_aux_bit(Timestamp::aux_to_bit(next_par))
                .with_tid(tid)
        {
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
            return Ok(());
        }

        // CAS 성공한 뒤에 helping 받은 건지 체크
        let pchk = Timestamp::from(
            exec_info.cas_info.cas_help[Timestamp::aux_to_bit(next_par)][tid]
                .load(Ordering::SeqCst),
        );
        if vchk >= pchk {
            return Err(self.load_help(cur, exec_info, guard));
        }

        // 마지막 CAS보다 helper가 쓴 체크포인트가 높으므로 성공한 것
        // 이미 location의 값은 바뀌었으므로 내 checkpoint만 마무리
        mmt.checkpoint_succ(next_par, tid, exec_info);
        sfence();

        Ok(())
    }

    /// TODO(doc)
    #[inline]
    pub fn load<'g>(&self, ord: Ordering, guard: &'g Guard, pool: &PoolHandle) -> PShared<'g, N> {
        let cur = self.inner.load(ord, guard);
        self.load_help(cur, &pool.exec_info, guard)
    }

    const PATIENCE: u64 = 40000;

    /// return bool: 계속 진행 가능 여부 (`old`로 CAS를 해도 되는지 여부)
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

            let chk = loop {
                // get checkpoint timestamp
                let start = rdtscp();
                lfence();

                // start spin loop
                let out = loop {
                    let cur = self.inner.load(Ordering::SeqCst, guard);

                    // return if cur is clean. (previous chk timestamp is useless.)
                    if cur.tid() == 0 {
                        return cur;
                    }

                    // if old was changed, new spin loop needs to be started.
                    if old != cur {
                        old = cur;
                        break false;
                    }

                    // if patience is over, I have to help it.
                    let now = rdtscp();
                    if now > start + Self::PATIENCE {
                        break true;
                    }
                };

                if out {
                    break calc_checkpoint(start, exec_info);
                }
            };

            let winner_tid = old.tid();
            let winner_bit = old.aux_bit();

            // check if winner thread's pcheckpoint is stale
            let pchk = exec_info.cas_info.cas_help[winner_bit][winner_tid].load(Ordering::SeqCst);
            if chk <= pchk {
                // Someone may already help it. I should retry to load.
                old = self.inner.load(Ordering::SeqCst, guard);
                continue;
            }

            // persist the pointer before CASing winner thread's pcheckpoint
            persist_obj(&self.inner, false);

            // CAS winner thread's pcheckpoint
            if exec_info.cas_info.cas_help[winner_bit][winner_tid]
                .compare_exchange(pchk, chk, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                // Someone may already help it. I should retry to load.
                old = self.inner.load(Ordering::SeqCst, guard);
                continue;
            }

            // help pointer to be clean.
            persist_obj(&exec_info.cas_info.cas_help[winner_bit][winner_tid], false);
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

unsafe impl<N: Collectable> Send for DetectableCASAtomic<N> {}
unsafe impl<N: Collectable> Sync for DetectableCASAtomic<N> {}

#[derive(Debug)]
pub(crate) struct CasInfo {
    /// tid별 스스로 cas 성공한 시간
    pub(crate) cas_own: CASCheckpointArr,

    /// tid별 helping 받은 시간
    pub(crate) cas_help: &'static [CASCheckpointArr; 2],
}

impl From<&'static [CASCheckpointArr; 2]> for CasInfo {
    fn from(chk_ref: &'static [CASCheckpointArr; 2]) -> Self {
        Self {
            cas_own: array_init::array_init(|_| CachePadded::new(AtomicU64::new(0))),
            cas_help: chk_ref,
        }
    }
}

/// TODO(doc)
#[derive(Debug)]
pub struct Cas<N> {
    checkpoint: Timestamp,
    _marker: PhantomData<N>,
}

impl<N> Default for Cas<N> {
    fn default() -> Self {
        Self {
            checkpoint: Timestamp::new(false, NOT_CHECKED),
            _marker: Default::default(),
        }
    }
}

impl<N> Collectable for Cas<N> {
    fn filter(cas: &mut Self, tid: usize, _: &mut GarbageCollection, pool: &mut PoolHandle) {
        // CAS client 중 max checkpoint를 가진 걸로 vcheckpoint에 기록해줌
        let vchk = Timestamp::from(pool.exec_info.cas_info.cas_own[tid].load(Ordering::Relaxed));

        if cas.checkpoint > vchk {
            pool.exec_info.cas_info.cas_own[tid].store(cas.checkpoint.into(), Ordering::Relaxed);
        }
    }
}

impl<N> Cas<N> {
    #[inline]
    fn checkpoint_succ(&mut self, parity: bool, tid: usize, exec_info: &ExecInfo) {
        let t = calc_checkpoint(rdtscp(), &exec_info);
        let new_chk = Timestamp::new(parity, t);
        self.checkpoint = new_chk;
        persist_obj(&self.checkpoint, false); // There is always a CAS after this function
        exec_info.cas_info.cas_own[tid].store(new_chk.into(), Ordering::Relaxed);
        exec_info.local_max_time[tid].store(new_chk.into(), Ordering::Relaxed);
    }
}

#[inline]
fn calc_checkpoint(t: u64, exec_info: &ExecInfo) -> u64 {
    t - exec_info.init_time.time() + exec_info.global_max_time.time()
}
