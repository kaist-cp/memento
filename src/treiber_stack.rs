//! Persistent stack based on Treiber stack

use core::sync::atomic::{AtomicUsize, Ordering};

use crate::pepoch::atomic::Pointer;
use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::persistent::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::plocation::{ll::*, pool::*, AsPPtr};
use crate::stack::*;

// TODO: T가 포인터일 수 있으니 T도 Collectable이여야함
struct Node<T: Clone> {
    data: T,
    next: PAtomic<Node<T>>,

    /// push 되었는지 여부
    // 이게 없으면, pop()에서 node 뺀 후 popper 등록 전에 crash 났을 때, 노드가 이미 push 되었었다는 걸 알 수 없음
    pushed: bool,

    /// 누가 pop 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    popper: AtomicUsize,
}

impl<T: Clone> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: PAtomic::null(),
            pushed: false,
            popper: AtomicUsize::new(TreiberStack::<T>::no_popper()),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Node<T> {}
impl<T: Clone> Collectable for Node<T> {
    fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut next = node.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            let next_ref = unsafe { next.deref_mut(pool) };
            Node::<T>::mark(next_ref, gc);
        }
    }
}

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
    fn filter(try_push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut mine = try_push.mine.load(Ordering::SeqCst, guard);
        if !mine.is_null() {
            let mine_ref = unsafe { mine.deref_mut(pool) };
            Node::mark(mine_ref, gc);
        }
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
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        stack.push(self, value, guard, pool)
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        let mine = self.mine.load(Ordering::SeqCst, guard);
        if !mine.is_null() {
            self.mine.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.mine, true);

            // crash-free execution이지만 try_push라서 push 실패했을 수 있음
            // 따라서 pushed 플래그로 (1) 성공여부 확인후, (2) push 되지 않았으면 free
            //
            // NOTE:
            //  - 현재는 push CAS 성공 후 pushed=true로 설정해주니까, 성공했다면 pushed=true가 보장됨
            //  - 만약 최적화하며 push CAS 성공 후 pushed=true를 안하게 바꾼다면, 여기서는 pushed 대신 Token에 담겨있는 Ok or Err 정보로 성공여부 판단해야함 (혹은 Direct tracking..)
            if unsafe { !mine.deref(pool).pushed } {
                unsafe { guard.defer_pdestroy(mine) };
            }
        }
    }
}

impl<T: Clone> Drop for TryPush<T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let mine = self.mine.load(Ordering::SeqCst, guard);
        assert!(mine.is_null(), "reset 되어있지 않음.")
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
    fn filter(push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut mine = push.mine.load(Ordering::SeqCst, guard);
        if !mine.is_null() {
            let mine_ref = unsafe { mine.deref_mut(pool) };
            Node::<T>::mark(mine_ref, gc);
        }
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
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let pushed = stack.push(self, value, guard, pool);
        debug_assert!(pushed.is_ok());
        Ok(())
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, _: &'static PoolHandle) {
        let mine = self.mine.load(Ordering::SeqCst, guard);
        if !mine.is_null() {
            self.mine.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.mine, true);

            // crash-free execution이니 내가 가지고 있던 노드는 push 되었음이 확실 => free하면 안됨
        }
    }
}

impl<T: Clone> Drop for Push<T> {
    fn drop(&mut self) {
        let mine = self
            .mine
            .load(Ordering::SeqCst, unsafe { epoch::unprotected() });
        assert!(mine.is_null(), "reset 되어있지 않음.")
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
    /// pop를 위해 할당된 node
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
    fn filter(try_pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut target = try_pop.target.load(Ordering::SeqCst, guard);
        if !target.is_null() {
            let target_ref = unsafe { target.deref_mut(pool) };
            Node::<T>::mark(target_ref, gc);
        }
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
        guard: &mut Guard,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        stack.pop(self, guard, pool)
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        let target = self.target.load(Ordering::SeqCst, guard);

        if target.tag() == TryPop::<T>::EMPTY {
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);
            return;
        }

        if !target.is_null() {
            // null로 바꾼 후, free 하기 전에 crash 나도 상관없음.
            // root로부터 도달 불가능해졌다면 GC가 수거해갈 것임.
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);

            // crash-free execution이지만 try이니 popper가 내가 아닐 수 있음
            // 따라서 popper를 확인 후 내가 pop한게 맞다면 free
            if unsafe { target.deref(pool) }.popper.load(Ordering::SeqCst) == self.id(pool) {
                unsafe { guard.defer_pdestroy(target) };
            }
        }
    }
}

impl<T: Clone> TryPop<T> {
    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 1;
}

impl<T: Clone> Drop for TryPop<T> {
    fn drop(&mut self) {
        let target = self
            .target
            .load(Ordering::SeqCst, unsafe { epoch::unprotected() });
        assert!(target.is_null(), "reset 되어있지 않음.")
    }
}

/// TreiberStack의 pop operation
#[derive(Debug)]
pub struct Pop<T: Clone> {
    /// pop를 위해 할당된 node
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
    fn filter(pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut target = pop.target.load(Ordering::SeqCst, guard);
        if !target.is_null() {
            let target_ref = unsafe { target.deref_mut(pool) };
            Node::<T>::mark(target_ref, gc);
        }
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
        guard: &mut Guard,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        Ok(stack.pop(self, guard, pool).unwrap())
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, _: &'static PoolHandle) {
        let target = self.target.load(Ordering::SeqCst, guard);

        if target.tag() == TryPop::<T>::EMPTY {
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);
            return;
        }

        if !target.is_null() {
            // null로 바꾼 후, free 하기 전에 crash 나도 상관없음.
            // root로부터 도달 불가능해졌다면 GC가 수거해갈 것임.
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);

            // crash-free execution이고 try가 아니니 가리키는 노드는 내가 deq한 노드임이 확실 => 내가 free
            unsafe { guard.defer_pdestroy(target) };
        }
    }
}

impl<T: Clone> Drop for Pop<T> {
    fn drop(&mut self) {
        let target = self
            .target
            .load(Ordering::SeqCst, unsafe { epoch::unprotected() });
        assert!(target.is_null(), "reset 되어있지 않음.")
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

impl<T: Clone> Collectable for TreiberStack<T> {
    fn filter(stack: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut top = stack.top.load(Ordering::SeqCst, guard);
        if !top.is_null() {
            let top_ref = unsafe { top.deref_mut(pool) };
            Node::mark(top_ref, gc);
        }
    }
}

impl<T: Clone> TreiberStack<T> {
    fn push<C: PushType<T>>(
        &self,
        client: &C,
        value: T,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), TryFail> {
        let mut mine = client.mine().load(Ordering::SeqCst, guard);

        if mine.is_null() {
            // (1) mine이 null이면 node 할당이 안 된 것이다
            let n = POwned::new(Node::from(value), pool).into_shared(guard);

            client.mine().store(n, Ordering::SeqCst);
            persist_obj(client.mine(), true);
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
        mut node: PShared<'_, Node<T>>,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), TryFail> {
        let node_ref = unsafe { node.deref_mut(pool) };
        let top = self.top.load(Ordering::SeqCst, guard);

        node_ref.next.store(top, Ordering::SeqCst);
        persist_obj(&node_ref.next, true);
        self.top
            .compare_exchange(top, node, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| {
                persist_obj(&self.top, true);

                // @seungminjeon:
                // - Tracking in Order의 Elim-stack은 여기서 pushed=true로 표시해주지만, 이는 불필요한 write 같음
                // - 왜냐하면 push된 노드는 stack에 있거나 혹은 stack에 없으면(i.e. pop 되었으면) pushed=true 적혀있음이 보장되기 때문
                node_ref.pushed = true;
                persist_obj(&node_ref.pushed, true);
            })
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
                    persist_obj(&target_ref.popper, true);
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
        let mut top = self.top.load(Ordering::SeqCst, guard);
        if top.is_null() {
            // empty
            client.target().store(
                PShared::null().with_tag(TryPop::<T>::EMPTY),
                Ordering::SeqCst,
            );
            persist_obj(client.target(), true);
            return Ok(None);
        };
        let top_ref = unsafe { top.deref_mut(pool) };

        // 우선 내가 top node를 가리키고
        client.target().store(top, Ordering::SeqCst);
        persist_obj(client.target(), true);

        // node가 push된 노드라고 마킹해준 후
        top_ref.pushed = true;
        persist_obj(&top_ref.pushed, true);

        // top node를 next로 바꿈
        let next = top_ref.next.load(Ordering::SeqCst, guard);
        if self
            .top
            .compare_exchange(top, next, Ordering::SeqCst, Ordering::SeqCst, guard)
            .is_err()
        {
            return Err(TryFail);
        }

        persist_obj(&self.top, true);

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
            .map(|_| {
                persist_obj(&top_ref.popper, true);
                Some(Self::finish_pop(top_ref))
            })
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
