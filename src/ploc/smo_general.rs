use std::{
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
};

use crossbeam_epoch::Guard;

use crate::{
    pepoch::{PAtomic, PShared},
    pmem::{lfence, ll::persist_obj, rdtsc, rdtscp, Collectable, GarbageCollection, PoolHandle},
    Memento,
};

use super::NodeUnOpt;

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
    checkpoint: AtomicU64,
    _marker: PhantomData<N>,
}

impl<N> Default for Cas<N> {
    fn default() -> Self {
        Self {
            checkpoint: AtomicU64::new(Self::NOT_CHECKED),
            _marker: Default::default(),
        }
    }
}

impl<N> Collectable for Cas<N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<N> Memento for Cas<N>
where
    N: 'static + NodeUnOpt + Collectable,
{
    type Object<'o> = &'o PAtomic<N>;
    type Input<'o> = (PShared<'o, N>, PShared<'o, N>, &'o [AtomicU64; 256]); // atomicu64 array는 나중에 글로벌 배열로 빼야 함 maybe into poolhandle
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        target: Self::Object<'o>,
        (old, new, checkpoint): Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(new, checkpoint);
        }

        let tmp_new = new.with_tid(tid);
        target
            .compare_exchange(old, tmp_new, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| {
                // 성공하면 target을 persist
                persist_obj(target, true);

                // 그후 tid 뗀 포인터를 넣어줌으로써 checkpoint는 필요 없다고 알림
                let _ = target.compare_exchange(
                    tmp_new,
                    new.with_tid(0),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
                persist_obj(target, true);

                // 성공했다고 체크포인팅
                self.checkpoint.store(rdtscp(), Ordering::SeqCst);
                persist_obj(&self.checkpoint, true);
            })
            .map_err(|e| {
                let succ_tid = e.current.tid();

                if succ_tid == 0 {
                    return;
                }

                let t = rdtsc();
                lfence();
                let cur = target.load(Ordering::SeqCst, guard);

                if e.current != cur {
                    return;
                }

                let c = checkpoint[succ_tid].load(Ordering::SeqCst);
                if t <= c {
                    // 이미 누가 한 거임
                    return;
                }

                if checkpoint[succ_tid]
                    .compare_exchange(c, t, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    let _ = target.compare_exchange(
                        e.current,
                        e.current.with_tid(0),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                }
            })
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

impl<N> Cas<N> {
    const NOT_CHECKED: u64 = 0;

    fn result(&self, new: PShared<'_, N>, checkpoint: &[AtomicU64; 256]) -> Result<(), ()> {
        todo!()
    }
}
