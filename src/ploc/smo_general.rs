//! General SMO

use std::{
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
};

use chrono::{Duration, Utc};
use crossbeam_epoch::Guard;

use crate::{
    pepoch::{PAtomic, PShared},
    pmem::{
        lfence, ll::persist_obj, rdtsc, rdtscp, sfence, Collectable, GarbageCollection, PoolHandle,
    },
    Memento,
};

use super::{compose_cas_bit, decompose_cas_bit};

const NR_MAX_THREADS: usize = 512;

pub(crate) type CASCheckpointArr = [AtomicU64; NR_MAX_THREADS]; // TODO(opt): CachePadded?

#[derive(Debug)]
pub(crate) struct CasInfo {
    /// tid별 스스로 cas 성공한 시간
    cas_vcheckpoint: CASCheckpointArr,

    /// tid별 helping 받은 시간
    cas_pcheckpoint: &'static CASCheckpointArr,

    /// 지난 실행에서 최대 체크포인트 시간
    prev_max_checkpoint: u64,

    /// 프로그램 초기 시간
    timestamp_init: u64,
}

impl From<&'static CASCheckpointArr> for CasInfo {
    fn from(chk_ref: &'static CASCheckpointArr) -> Self {
        Self {
            cas_vcheckpoint: array_init::array_init(|_| AtomicU64::new(0)),
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
        let max = self.cas_pcheckpoint.iter().fold(max, |m, chk| {
            let t = chk.load(Ordering::Relaxed);
            std::cmp::max(m, t)
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
            checkpoint: Self::NOT_CHECKED,
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

impl<N> Memento for Cas<N>
where
    N: 'static,
{
    type Object<'o> = &'o PAtomic<N>;
    type Input<'o> = (PShared<'o, N>, PShared<'o, N>);
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        target: Self::Object<'o>,
        (old, new): Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(target, new, tid, guard, &pool.cas_info);
        }

        if !Self::help(target, old, guard, &pool.cas_info) {
            return Err(());
        }

        let prev_chk = pool.cas_info.cas_vcheckpoint[tid].load(Ordering::Relaxed);
        let cas_bit = 1 - decompose_cas_bit(prev_chk as usize).0;
        let tmp_new = new.with_cas_bit(cas_bit).with_tid(tid);

        target
            .compare_exchange(old, tmp_new, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| {
                // 성공하면 target을 persist
                persist_obj(target, true);

                // 성공했다고 체크포인팅
                self.checkpoint_succ(cas_bit, tid, &pool.cas_info);

                // 그후 tid 뗀 포인터를 넣어줌으로써 checkpoint는 필요 없다고 알림
                let _ = target
                    .compare_exchange(
                        tmp_new,
                        new.with_tid(0),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    )
                    .map_err(|_| sfence()); // cas 실패시 synchronous flush를 위해 sfence 해줘야 함
            })
            .map_err(|_| ())
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        self.checkpoint = Self::NOT_CHECKED;
        persist_obj(&self.checkpoint, false);
    }
}

impl<N> Cas<N> {
    const NOT_CHECKED: u64 = 0;

    #[inline]
    fn result(
        &mut self,
        target: &PAtomic<N>,
        new: PShared<'_, N>,
        tid: usize,
        guard: &Guard,
        cas_info: &CasInfo,
    ) -> Result<(), ()> {
        let cur = target.load(Ordering::SeqCst, guard);
        if self.checkpoint != Self::NOT_CHECKED {
            if cur.with_cas_bit(0) == new.with_cas_bit(0).with_tid(tid) {
                let _ = target.compare_exchange(
                    cur,
                    new.with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            }

            return Ok(());
        }

        // CAS 성공하고 죽었는지 체크
        let vchk = cas_info.cas_vcheckpoint[tid].load(Ordering::Relaxed);
        let (vbit, vchk) = decompose_cas_bit(vchk as usize);
        let pchk = cas_info.cas_pcheckpoint[tid].load(Ordering::SeqCst);
        let (pbit, pchk) = decompose_cas_bit(pchk as usize);

        // 마지막 CAS보다 helper 쓴 체크포인트가 높아야 하고 && 마지막 홀짝도 다르면 성공한 것
        if vchk < pchk && vbit != pbit {
            self.checkpoint_succ(pbit, tid, cas_info);
            return Ok(());
        }

        Err(())
    }

    #[inline]
    fn calc_checkpoint(t: u64, cas_info: &CasInfo) -> u64 {
        t - cas_info.timestamp_init + cas_info.prev_max_checkpoint
    }

    #[inline]
    fn checkpoint_succ(&mut self, cas_bit: usize, tid: usize, cas_info: &CasInfo) {
        let t = Self::calc_checkpoint(rdtscp(), cas_info);
        let new_chk = compose_cas_bit(cas_bit, t as usize) as u64;
        self.checkpoint = new_chk;
        persist_obj(&self.checkpoint, true);
        cas_info.cas_vcheckpoint[tid].store(new_chk, Ordering::Relaxed);
    }

    /// return bool: 계속 진행 가능 여부 (`old`로 CAS를 해도 되는지 여부)
    #[inline]
    fn help<'g>(
        target: &PAtomic<N>,
        old: PShared<'_, N>,
        guard: &'g Guard,
        cas_info: &CasInfo,
    ) -> bool {
        let succ_tid = old.tid();

        if succ_tid == 0 {
            return true;
        }

        let now = Self::calc_checkpoint(rdtsc(), cas_info);
        lfence();

        let start = Utc::now();
        loop {
            let cur = target.load(Ordering::SeqCst, guard);
            if old != cur {
                return false;
            }
            let now = Utc::now();
            if now.signed_duration_since(start) > Duration::nanoseconds(4000) {
                break;
            }
        }

        let pchk = cas_info.cas_pcheckpoint[succ_tid].load(Ordering::SeqCst);
        let pchk_time = decompose_cas_bit(pchk as usize).1;
        if now <= pchk_time as u64 {
            // 이미 누가 한 거임
            return false;
        }

        persist_obj(target, false);

        let now = compose_cas_bit(old.cas_bit(), now as usize) as u64;
        if cas_info.cas_pcheckpoint[succ_tid]
            .compare_exchange(pchk, now, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            persist_obj(&cas_info.cas_pcheckpoint[succ_tid], true);
            return true;
        }

        false
    }
}
