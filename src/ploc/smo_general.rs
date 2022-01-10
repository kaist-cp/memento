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

// link-and-checkpoint (general CAS 지원)
// assumption: 각 thread는 CAS checkpoint를 위한 64-bit PM location이 있습니다. 이를 checkpoint: [u64; 256] 이라고 합시다.

// step 1 (link): CAS from old node to new node pointer (w/ thread id, app tags, ptr address) 및 persist
// step 2 (checkpoint): client에 성공/실패 여부 기록 및 persist
// step 3 (persist): CAS로 link에서 thread id 제거 및 persist

// concurrent thread reading the link: link에 thread id가 남아있으면
// 그 thread id를 포함한 u64 value를 checkpoint[tid]에 store & persist하고나서 CAS로 link에서 thread id 제거 및 perist

// recovery run: client에 성공/실패가 기록되어있으면 바로 return;
// location이 new value면 step 2에서 resume; new value가 checkpoint[tid]와 같으면 성공으로 기록하고 return; 아니면 step 1에서 resume.
// 이게 가능한 이유는 내가 아직 thread id를 지우지 않은 CAS는 존재한다면 유일하기 때문입니다.
// 따라서 다른 thread는 checkpoint에 그냥 store를 해도 됩니다.

// 사용처: 아무데나

/// TODO(doc)
#[derive(Debug)]
pub struct Cas<N> {
    // TODO: N: Node 정의
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
        let vchk = pool.cas_vcheckpoint[tid].load(Ordering::Relaxed);
        let (_, cur_chk) = decompose_cas_bit(cas.checkpoint as usize);
        let (_, max_chk) = decompose_cas_bit(vchk as usize);

        if cur_chk > max_chk {
            pool.cas_vcheckpoint[tid].store(cas.checkpoint, Ordering::Relaxed);
        }
    }
}

impl<N> Memento for Cas<N>
where
    N: 'static + Collectable,
{
    type Object<'o> = &'o PAtomic<N>;
    type Input<'o> = (PShared<'o, N>, PShared<'o, N>, &'o [AtomicU64; 256]); // atomicu64 array는 나중에 글로벌 배열로 빼야 함 maybe into poolhandle
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        target: Self::Object<'o>,
        (old, new, pcheckpoint): Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(
                target,
                new,
                tid,
                &pool.cas_vcheckpoint[tid],
                &pcheckpoint[tid],
                guard,
                pool,
            );
        }

        if !Self::help(target, old, pcheckpoint, guard) {
            return Err(());
        }

        let prev_chk = pool.cas_vcheckpoint[tid].load(Ordering::Relaxed);
        let cas_bit = 1 - decompose_cas_bit(prev_chk as usize).0;
        let tmp_new = new.with_cas_bit(cas_bit).with_tid(tid);

        target
            .compare_exchange(old, tmp_new, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| {
                // 성공하면 target을 persist
                persist_obj(target, true);

                // 성공했다고 체크포인팅
                self.checkpoint_succ(cas_bit, tid, pool);

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
        // TODO
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
        vcheckpoint: &AtomicU64,
        pcheckpoint: &AtomicU64,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let cur = target.load(Ordering::SeqCst, guard);
        if self.checkpoint != Self::NOT_CHECKED {
            if cur == new.with_tid(tid) {
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
        let vchk = vcheckpoint.load(Ordering::Relaxed);
        let (vbit, vchk) = decompose_cas_bit(vchk as usize);
        let pchk = pcheckpoint.load(Ordering::SeqCst);
        let (pbit, pchk) = decompose_cas_bit(pchk as usize);

        // 마지막 CAS보다 helper 쓴 체크포인트가 높아야 하고 && 마지막 홀짝도 다르면 성공한 것
        if vchk < pchk && vbit != pbit {
            self.checkpoint_succ(pbit, tid, pool);
            return Ok(());
        }

        Err(())
    }

    #[inline]
    fn checkpoint_succ(&mut self, cas_bit: usize, tid: usize, pool: &PoolHandle) {
        let t = rdtscp();
        let new_chk = compose_cas_bit(cas_bit, t as usize) as u64;
        self.checkpoint = new_chk;
        persist_obj(&self.checkpoint, true);
        pool.cas_vcheckpoint[tid].store(new_chk, Ordering::Relaxed);
    }

    /// return bool: 계속 진행 가능 여부 (`old`로 CAS를 해도 되는지 여부)
    #[inline]
    fn help<'g>(
        target: &PAtomic<N>,
        old: PShared<'_, N>,
        pcheckpoint: &[AtomicU64; 256],
        guard: &'g Guard,
    ) -> bool {
        let succ_tid = old.tid();

        if succ_tid == 0 {
            return true;
        }

        let now = rdtsc();
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

        let cur = target.load(Ordering::SeqCst, guard);

        if old != cur {
            return false;
        }

        let pchk = pcheckpoint[succ_tid].load(Ordering::SeqCst);
        let pchk_time = decompose_cas_bit(pchk as usize).1;
        if now <= pchk_time as u64 {
            // 이미 누가 한 거임
            return false;
        }

        persist_obj(target, false);

        let now = compose_cas_bit(old.cas_bit(), now as usize) as u64;
        if pcheckpoint[succ_tid]
            .compare_exchange(pchk, now, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            persist_obj(&pcheckpoint[succ_tid], true);
            // let res = target.compare_exchange(
            //     old,
            //     old.with_tid(0),
            //     Ordering::SeqCst,
            //     Ordering::SeqCst,
            //     guard,
            // );

            // if let Err(e) = res {
            //     if e.current == old.with_tid(0) {
            //         return true;
            //     }
            // } else {
            //     return true;
            // }

            return true;
        }

        false
    }
}
