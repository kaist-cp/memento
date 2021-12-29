use std::{marker::PhantomData, sync::atomic::Ordering};

use crossbeam_epoch::Guard;

use crate::{
    pepoch::{PAtomic, PShared},
    pmem::{Collectable, GarbageCollection, PoolHandle},
    Memento,
};

use super::{NodeUnOpt, Traversable};

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
    checkpoint: usize,
    _marker: PhantomData<*const N>,
}

impl<N> Default for Cas<N> {
    fn default() -> Self {
        todo!()
    }
}

impl<N> Collectable for Cas<N> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<N> Memento for Cas<N>
where
    N: 'static + NodeUnOpt + Collectable,
{
    type Object<'o> = &'o PAtomic<N>;
    type Input<'o> = (PShared<'o, N>, PShared<'o, N>);
    type Output<'o>
    where
        N: 'o,
    = ();
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        target: Self::Object<'o>,
        (old, new): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result();
        }

        let res = target.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard);
        if res.is_ok() {
            target
        }

        todo!()
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

impl<N> Cas<N> {
    fn result(&self) {
        todo!();
    }
}
