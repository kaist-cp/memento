//! General SMO

use std::{
    cell::RefCell,
    cmp,
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

use super::{compose_cas_bit, decompose_cas_bit};

const NOT_CHECKED: u64 = 0;
const FAILED: u64 = 1;
const NR_MAX_THREADS: usize = 511;

pub(crate) type CASCheckpointArr = [CachePadded<AtomicU64>; NR_MAX_THREADS + 1];

thread_local! {
    /// 마지막 실패한 cas의 체크포인트
    static LAST_FAILED_CAS: RefCell<Option<PPtr<u64>>> = RefCell::new(None)
}

/// TODO(doc)
#[derive(Debug)]
pub struct DetectableCASAtomic<N: Collectable> {
    pub inner: PAtomic<N>,
}

impl<N: Collectable> Collectable for DetectableCASAtomic<N> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
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
            return self.cas_result(new, mmt, tid, &pool.cas_info, guard);
        }

        LAST_FAILED_CAS.with(|c| {
            let mut failed = c.borrow_mut();
            if let Some(last_chk) = *failed {
                unsafe {
                    if last_chk != mmt.checkpoint.as_pptr(pool) {
                        let last_chk_ref = last_chk.deref_mut(pool);
                        std::ptr::write(last_chk_ref as *mut _, FAILED);
                        persist_obj(last_chk_ref as &_, true);
                    } else {
                        *failed = None;
                    }
                }
            }
        });

        let prev_chk = pool.cas_info.cas_vcheckpoint[tid].load(Ordering::Relaxed);
        let cas_bit = 1 - decompose_cas_bit(prev_chk as usize).0;
        let tmp_new = new.with_cas_bit(cas_bit).with_tid(tid);

        loop {
            let res = self
                .inner
                .compare_exchange(old, tmp_new, Ordering::SeqCst, Ordering::SeqCst, guard)
                .map(|_| {
                    // 성공하면 target을 persist
                    persist_obj(&self.inner, true);

                    // 성공했다고 체크포인팅
                    mmt.checkpoint_succ(cas_bit, tid, &pool.cas_info);
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
                })
                .map_err(|e| self.help(e.current, &pool.cas_info, guard));

            if let Err(cur) = res {
                // retry for the property of strong CAS
                if cur == old {
                    continue;
                }

                LAST_FAILED_CAS.with(|failed| {
                    *failed.borrow_mut() = Some(unsafe { mmt.checkpoint.as_pptr(pool) });
                });
            }

            return res;
        }
    }

    /// TODO(doc)
    #[inline]
    pub fn load<'g>(&self, ord: Ordering, guard: &'g Guard, pool: &PoolHandle) -> PShared<'g, N> {
        let cur = self.inner.load(ord, guard);
        self.help(cur, &pool.cas_info, guard)
    }

    #[inline]
    fn cas_result<'g>(
        &self,
        new: PShared<'_, N>,
        mmt: &mut Cas<N>,
        tid: usize,
        cas_info: &CasInfo,
        guard: &'g Guard,
    ) -> Result<(), PShared<'g, N>> {
        if mmt.checkpoint == FAILED {
            let cur = self.inner.load(Ordering::SeqCst, guard);
            return Err(self.help(cur, cas_info, guard)); // TODO(opt): RecFail?
        }

        let vchk = cas_info.cas_vcheckpoint[tid].load(Ordering::Relaxed);
        let (cur_bit, max_chk) = decompose_cas_bit(vchk as usize);
        let next_bit = 1 - cur_bit;

        if mmt.checkpoint != NOT_CHECKED {
            let (_, cli_chk) = decompose_cas_bit(mmt.checkpoint as usize);

            match cli_chk.cmp(&max_chk) {
                cmp::Ordering::Less => return Ok(()),
                cmp::Ordering::Equal => {
                    let _ = self.inner.compare_exchange(
                        new.with_cas_bit(cur_bit).with_tid(tid),
                        new.with_tid(0), // TODO(opt): 깔끔한 cas_bit?
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                    return Ok(());
                }
                cmp::Ordering::Greater => {
                    // Occured when thread crash. Just continue below... (It should return `Ok` finally)
                }
            }
        }

        let cur = self.inner.load(Ordering::SeqCst, guard);

        if cur == new.with_cas_bit(next_bit).with_tid(tid) {
            mmt.checkpoint_succ(next_bit, tid, cas_info);
            let _ = self
                .inner
                .compare_exchange(
                    cur,
                    new.with_tid(0), // TODO(opt): 깔끔한 cas_bit?
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                )
                .map_err(|_| sfence);
            return Ok(());
        }

        // CAS 성공하고 죽었는지 체크
        let pchk = cas_info.cas_pcheckpoint[next_bit][tid].load(Ordering::SeqCst);

        // 마지막 CAS보다 helper 쓴 체크포인트가 높아야 하고 && 마지막 홀짝도 다르면 성공한 것
        if max_chk < pchk as usize {
            mmt.checkpoint_succ(next_bit, tid, cas_info);
            let _ = self
                .inner
                .compare_exchange(
                    new.with_cas_bit(next_bit).with_tid(tid),
                    new.with_tid(0), // TODO(opt): 깔끔한 cas_bit?
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                )
                .map_err(|_| sfence);
            return Ok(());
        }

        Err(self.help(cur, cas_info, guard))
    }

    const PATIENCE: u64 = 40000;

    /// return bool: 계속 진행 가능 여부 (`old`로 CAS를 해도 되는지 여부)
    #[inline]
    fn help<'g>(
        &self,
        mut old: PShared<'g, N>,
        cas_info: &CasInfo,
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
                    break calc_checkpoint(start, cas_info);
                }
            };

            let winner_tid = old.tid();
            let winner_bit = old.cas_bit();

            // check if winner thread's pcheckpoint is stale
            let pchk = cas_info.cas_pcheckpoint[winner_bit][winner_tid].load(Ordering::SeqCst);
            let pchk_time = decompose_cas_bit(pchk as usize).1;
            if chk <= pchk_time as u64 {
                // Someone may already help it. I should retry to load.
                old = self.inner.load(Ordering::SeqCst, guard);
                continue;
            }

            // persist the pointer before CASing winner thread's pcheckpoint
            persist_obj(&self.inner, false);

            // CAS winner thread's pcheckpoint
            if cas_info.cas_pcheckpoint[winner_bit][winner_tid]
                .compare_exchange(pchk, chk, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                // Someone may already help it. I should retry to load.
                old = self.inner.load(Ordering::SeqCst, guard);
                continue;
            }

            // help pointer to be clean.
            persist_obj(&cas_info.cas_pcheckpoint[winner_bit][winner_tid], false);
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
    cas_vcheckpoint: CASCheckpointArr,

    /// tid별 helping 받은 시간
    cas_pcheckpoint: &'static [CASCheckpointArr; 2],

    /// 지난 실행에서 최대 체크포인트 시간
    prev_max_checkpoint: u64,

    /// 프로그램 초기 시간
    timestamp_init: u64,
}

impl From<&'static [CASCheckpointArr; 2]> for CasInfo {
    fn from(chk_ref: &'static [CASCheckpointArr; 2]) -> Self {
        Self {
            cas_vcheckpoint: array_init::array_init(|_| CachePadded::new(AtomicU64::new(0))),
            cas_pcheckpoint: chk_ref,
            prev_max_checkpoint: 0,
            timestamp_init: rdtscp(),
        }
    }
}

impl CasInfo {
    #[inline]
    pub(crate) fn set_runtime_info(&mut self) {
        let max = self.cas_vcheckpoint.iter().fold(0, |m, chk| {
            let t = chk.load(Ordering::Relaxed);
            std::cmp::max(m, t)
        });
        let max = self.cas_pcheckpoint.iter().fold(max, |m, chk_arr| {
            chk_arr.iter().fold(m, |mm, chk| {
                let t = chk.load(Ordering::Relaxed);
                std::cmp::max(mm, t)
            })
        });

        self.prev_max_checkpoint = max;
    }
}

/// TODO(doc)
#[derive(Debug)]
pub struct Cas<N> {
    checkpoint: u64,
    _marker: PhantomData<N>,
}

impl<N> Default for Cas<N> {
    fn default() -> Self {
        Self {
            checkpoint: NOT_CHECKED,
            _marker: Default::default(),
        }
    }
}

impl<N> Collectable for Cas<N> {
    fn filter(cas: &mut Self, tid: usize, _: &mut GarbageCollection, pool: &PoolHandle) {
        // CAS client 중 max checkpoint를 가진 걸로 vcheckpoint에 기록해줌
        let vchk = pool.cas_info.cas_vcheckpoint[tid].load(Ordering::Relaxed);
        let (_, cur_chk) = decompose_cas_bit(cas.checkpoint as usize);
        let (_, max_chk) = decompose_cas_bit(vchk as usize);

        if cur_chk > max_chk {
            pool.cas_info.cas_vcheckpoint[tid].store(cas.checkpoint, Ordering::Relaxed);
        }
    }
}

impl<N> Cas<N> {
    /// TODO(doc)
    #[inline]
    pub fn reset(&mut self) {
        self.checkpoint = NOT_CHECKED;
        persist_obj(&self.checkpoint, false);
    }

    #[inline]
    fn checkpoint_succ(&mut self, cas_bit: usize, tid: usize, cas_info: &CasInfo) {
        let t = calc_checkpoint(rdtscp(), cas_info);
        let new_chk = compose_cas_bit(cas_bit, t as usize) as u64;
        self.checkpoint = new_chk;
        persist_obj(&self.checkpoint, false); // There is always a CAS after this function
        cas_info.cas_vcheckpoint[tid].store(new_chk, Ordering::Relaxed);
    }
}

#[inline]
fn calc_checkpoint(t: u64, cas_info: &CasInfo) -> u64 {
    t - cas_info.timestamp_init + cas_info.prev_max_checkpoint
}
