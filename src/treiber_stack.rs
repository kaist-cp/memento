//! Persistent stack based on Treiber stack

// TODO(SMR 적용):
// - SMR 만든 후 crossbeam 걷어내기
// - 현재는 persistent guard가 없어서 lifetime도 이상하게 박혀 있음

// TODO(pmem 사용(#31, #32)):
// - persist를 위해 flush/fence 추가
// - persistent location 위에서 동작

// TODO(Ordering):
// - Ordering 최적화

use core::sync::atomic::{AtomicUsize, Ordering};
use etrace::some_or;

use crate::pepoch::atomic::Pointer;
use crate::pepoch::{self as epoch, Guard, PAtomic, POwned, PShared};
use crate::persistent::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::plocation::{pool::*, AsPPtr};
use crate::stack::*;

struct Node<T: Clone> {
    data: T,
    next: PAtomic<Node<T>>,

    /// 누가 pop 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    popper: AtomicUsize,
}

impl<T: Clone> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: PAtomic::null(),
            popper: AtomicUsize::new(TreiberStack::<T>::no_popper()),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Node<T> {}

trait PushType<T: Clone> {
    fn mine(&self) -> &PAtomic<Node<T>>;
    fn is_try(&self) -> bool;
}

/// TreiberStack의 try push operation
#[derive(Debug)]
pub struct TryPush<T: Clone> {
    /// push를 위해 할당된 node
    mine: PAtomic<Node<T>>,
}

impl<T: Clone> Default for TryPush<T> {
    fn default() -> Self {
        Self {
            mine: PAtomic::null(),
        }
    }
}

impl<T: Clone> PushType<T> for TryPush<T> {
    #[inline]
    fn mine(&self) -> &PAtomic<Node<T>> {
        &self.mine
    }

    #[inline]
    fn is_try(&self) -> bool {
        true
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryPush<T> {}

impl<T: Clone> Collectable for TryPush<T> {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> Memento for TryPush<T> {
    type Object<'s> = &'s TreiberStack<T>;
    type Input = T;
    type Output<'s> = ();
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        value: Self::Input,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin();
        stack.push(self, value, &guard, pool)
    }

    fn reset(&mut self, _: bool, _: &PoolHandle) {
        // TODO: if not finished -> free node
        self.mine.store(PShared::null(), Ordering::SeqCst);
    }
}

/// TreiberStack의 push operation
#[derive(Debug)]
pub struct Push<T: Clone> {
    /// push를 위해 할당된 node
    mine: PAtomic<Node<T>>,
}

impl<T: Clone> Default for Push<T> {
    fn default() -> Self {
        Self {
            mine: PAtomic::null(),
        }
    }
}

impl<T: Clone> PushType<T> for Push<T> {
    #[inline]
    fn mine(&self) -> &PAtomic<Node<T>> {
        &self.mine
    }

    #[inline]
    fn is_try(&self) -> bool {
        false
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Push<T> {}

impl<T: Clone> Collectable for Push<T> {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> Memento for Push<T> {
    type Object<'s> = &'s TreiberStack<T>;
    type Input = T;
    type Output<'s> = ();
    type Error = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        value: Self::Input,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin();
        let pushed = stack.push(self, value, &guard, pool);
        debug_assert!(pushed.is_ok());
        Ok(())
    }

    fn reset(&mut self, _: bool, _: &PoolHandle) {
        // TODO: if not finished -> free node
        self.mine.store(PShared::null(), Ordering::SeqCst);
    }
}

trait PopType<T: Clone>: Sized {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }

    fn target(&self) -> &PAtomic<Node<T>>;
    fn is_try(&self) -> bool;
}

/// TreiberStack의 try pop operation
#[derive(Debug)]
pub struct TryPop<T: Clone> {
    /// pup를 위해 할당된 node
    target: PAtomic<Node<T>>,
}

impl<T: Clone> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            target: PAtomic::null(),
        }
    }
}

impl<T: Clone> PopType<T> for TryPop<T> {
    #[inline]
    fn target(&self) -> &PAtomic<Node<T>> {
        &self.target
    }

    #[inline]
    fn is_try(&self) -> bool {
        true
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryPop<T> {}

impl<T: Clone> Collectable for TryPop<T> {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> Memento for TryPop<T> {
    type Object<'s> = &'s TreiberStack<T>;
    type Input = ();
    type Output<'s> = Option<T>;
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin();
        stack.pop(self, &guard, pool)
    }

    fn reset(&mut self, _: bool, _: &PoolHandle) {
        // TODO: if node has not been freed, check if the node is mine and free it
        // TODO: null이 아닐 때에만 store하는 게 더 빠를까?
        self.target.store(PShared::null(), Ordering::SeqCst);
    }
}

impl<T: Clone> TryPop<T> {
    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 1;
}

/// TreiberStack의 pop operation
#[derive(Debug)]
pub struct Pop<T: Clone> {
    /// pup를 위해 할당된 node
    target: PAtomic<Node<T>>,
}

impl<T: Clone> Default for Pop<T> {
    fn default() -> Self {
        Self {
            target: PAtomic::null(),
        }
    }
}

impl<T: Clone> PopType<T> for Pop<T> {
    #[inline]
    fn target(&self) -> &PAtomic<Node<T>> {
        &self.target
    }

    #[inline]
    fn is_try(&self) -> bool {
        false
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Pop<T> {}

impl<T: Clone> Collectable for Pop<T> {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> Memento for Pop<T> {
    type Object<'s> = &'s TreiberStack<T>;
    type Input = ();
    type Output<'s> = Option<T>;
    type Error = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin();
        Ok(stack.pop(self, &guard, pool).unwrap())
    }

    fn reset(&mut self, _: bool, _: &PoolHandle) {
        // TODO: if node has not been freed, check if the node is mine and free it
        self.target.store(PShared::null(), Ordering::SeqCst);
    }
}

/// Persistent Treiber stack
// TODO: persist 추가
#[derive(Debug)]
pub struct TreiberStack<T: Clone> {
    top: PAtomic<Node<T>>,
}

impl<T: Clone> Default for TreiberStack<T> {
    fn default() -> Self {
        Self {
            top: PAtomic::null(),
        }
    }
}

impl<T: Clone> TreiberStack<T> {
    fn push<C: PushType<T>>(
        &self,
        client: &C,
        value: T,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), TryFail> {
        let mut mine = client.mine().load(Ordering::SeqCst, guard);

        if mine.is_null() {
            // (1) mine이 null이면 node 할당이 안 된 것이다
            let n = POwned::new(Node::from(value), pool).into_shared(guard);

            client.mine().store(n, Ordering::SeqCst);
            mine = n;
        } else if self.search(mine, guard, pool)
            || unsafe { mine.deref(pool) }.popper.load(Ordering::SeqCst) != Self::no_popper()
        {
            // TODO: recovery 중에만 분기 타도록
            // (3) stack 안에 mine이 있으면 push된 것이다 (Direct tracking)
            // (4) 이미 pop 되었다면 push된 것이다
            return Ok(());
        }

        if client.is_try() {
            self.try_push(mine, guard, pool)
        } else {
            while self.try_push(mine, guard, pool).is_err() {}
            Ok(())
        }
    }

    /// top에 새 `node` 연결을 시도
    fn try_push(
        &self,
        node: PShared<'_, Node<T>>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), TryFail> {
        let node_ref = unsafe { node.deref(pool) };
        let top = self.top.load(Ordering::SeqCst, guard);

        node_ref.next.store(top, Ordering::SeqCst);
        self.top
            .compare_exchange(top, node, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| ())
            .map_err(|_| TryFail)
    }

    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(&self, node: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut curr = self.top.load(Ordering::SeqCst, guard);

        while !curr.is_null() {
            if curr == node {
                return true;
            }

            let curr_ref = unsafe { curr.deref(pool) };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }

    fn pop<C: PopType<T>>(
        &self,
        client: &mut C,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<Option<T>, TryFail> {
        let target = client.target().load(Ordering::SeqCst, guard);

        if target.tag() == TryPop::<T>::EMPTY {
            // post-crash execution (empty)
            return Ok(None);
        }

        if !target.is_null() {
            // post-crash execution (trying)
            let target_ref = unsafe { target.deref(pool) };

            // target이 내가 pop한 게 맞는지 확인
            if target_ref.popper.load(Ordering::SeqCst) == client.id(pool) {
                return Ok(Some(Self::finish_pop(target_ref)));
            };

            // target이 stack에서 pop되긴 했는지 확인
            // TODO: recovery 중에만 분기 타도록
            if !self.search(target, guard, pool) {
                // 누군가가 target을 stack에서 빼고 popper 기록 전에 crash가 남. 그러므로 popper를 마저 기록해줌
                // CAS인 이유: 서로 누가 진짜 주인인 줄 모르고 모두가 복구하면서 같은 target을 노리고 있을 수 있음
                if target_ref
                    .popper
                    .compare_exchange(
                        Self::no_popper(),
                        client.id(pool),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    return Ok(Some(Self::finish_pop(target_ref)));
                }
            }
        }

        if client.is_try() {
            return self.try_pop(client, guard, pool);
        }

        loop {
            let result = self.try_pop(client, guard, pool);
            if result.is_ok() {
                return result;
            }
        }
    }

    /// top node를 pop 시도
    fn try_pop<C: PopType<T>>(
        &self,
        client: &C,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<Option<T>, TryFail> {
        let top = self.top.load(Ordering::SeqCst, guard);
        let top_ref = some_or!(unsafe { top.as_ref(pool) }, {
            // empty
            client.target().store(
                PShared::null().with_tag(TryPop::<T>::EMPTY),
                Ordering::SeqCst,
            );
            return Ok(None);
        });

        // 우선 내가 top node를 가리키고
        client.target().store(top, Ordering::SeqCst);

        // top node를 next로 바꿈
        let next = top_ref.next.load(Ordering::SeqCst, guard);
        if self
            .top
            .compare_exchange(top, next, Ordering::SeqCst, Ordering::SeqCst, guard)
            .is_err()
        {
            return Err(TryFail);
        }

        // top node에 내 이름 새겨넣음
        // CAS인 이유: pop 복구 중인 스레드와 경합이 일어날 수 있음
        top_ref
            .popper
            .compare_exchange(
                Self::no_popper(),
                client.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| Some(Self::finish_pop(top_ref)))
            .map_err(|_| TryFail)
    }

    fn finish_pop(node: &Node<T>) -> T {
        node.data.clone()
    }

    #[inline]
    fn no_popper() -> usize {
        let null = PShared::<TryPop<T>>::null();
        null.into_usize()
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TreiberStack<T> {}

impl<T: 'static + Clone> Stack<T> for TreiberStack<T> {
    type TryPush = TryPush<T>;
    type Push = Push<T>;
    type TryPop = TryPop<T>;
    type Pop = Pop<T>;
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::{stack::tests::*, utils::tests::*};

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn push_pop() {
        const FILE_NAME: &str = "treiber_push_pop.pool";
        run_test::<PushPop<TreiberStack<usize>, NR_THREAD, COUNT>, _>(FILE_NAME, FILE_SIZE)
    }
}
