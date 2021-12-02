//! Persistent stack based on Treiber stack

use core::sync::atomic::{AtomicUsize, Ordering};

use crate::atomic_update::{Acked, Insert, Traversable};
use crate::pepoch::atomic::Pointer;
use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, PShared};
use crate::persistent::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::plocation::{ll::*, pool::*, AsPPtr};
use crate::stack::*;

// TODO: T가 포인터일 수 있으니 T도 Collectable이여야함
#[derive(Debug)]
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

impl<T: Clone> Acked for Node<T> {
    fn acked(&self) -> bool {
        self.pushed
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

/// TreiberStack의 try push operation
#[derive(Debug)]
pub struct TryPush<T: Clone> {
    /// push를 위해 할당된 node
    insert: Insert<TreiberStack<T>, Node<T>>,
}

impl<T: Clone> Default for TryPush<T> {
    fn default() -> Self {
        Self {
            insert: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryPush<T> {}

impl<T: Clone> Collectable for TryPush<T> {
    fn filter(try_push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Insert::filter(&mut try_push.insert, gc, pool);
    }
}

impl<T: Clone> TryPush<T> {
    fn before_cas(mine: &mut Node<T>, oldtop: PShared<'_, Node<T>>) {
        mine.next.store(oldtop, Ordering::SeqCst);
        persist_obj(&mine.next, true);
    }
}

impl<T: 'static + Clone> Memento for TryPush<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = T;
    type Output<'o> = ();
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        value: Self::Input<'o>,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        self.insert
            .run(
                stack,
                (Node::from(value), &stack.top, Self::before_cas),
                guard,
                pool,
            )
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, nested: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        // 원래 하위 memento를 reset할 경우 reset flag를 쓰는 게 도리에 맞으나
        // `Insert`의 `reset()`이 atomic 하므로 안 써도 됨
        self.insert.reset(nested, guard, pool);
    }

    fn set_recovery(&mut self, pool: &'static PoolHandle) {
        self.insert.set_recovery(pool);
    }
}

impl<T: Clone> Drop for TryPush<T> {
    fn drop(&mut self) {
        todo!("하위 메멘토의 `is_reset()`이 필요함")
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
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
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

    fn set_recovery(&mut self, _: &'static PoolHandle) {
        let guard = unsafe { epoch::unprotected() };
        let target = self.target.load(Ordering::SeqCst, guard);

        let tag = target.tag();
        if tag & Self::EMPTY != Self::EMPTY && tag & Self::RECOVERY != Self::RECOVERY {
            self.target()
                .store(target.with_tag(Self::RECOVERY), Ordering::SeqCst);
            // 복구해야 한다는 표시이므로 persist 필요 없음
        }
    }
}

impl<T: Clone> TryPop<T> {
    const DEFAULT: usize = 0;

    /// Direct tracking 검사를 하게 만들도록 하는 복구중 태그
    const RECOVERY: usize = 1;

    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;
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
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
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

    fn set_recovery(&mut self, _: &'static PoolHandle) {}
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

impl<T: Clone> PDefault for TreiberStack<T> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Self::default()
    }
}

impl<T: Clone> Traversable<Node<T>> for TreiberStack<T> {
    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(&self, target: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut curr = self.top.load(Ordering::SeqCst, guard);

        while !curr.is_null() {
            if curr == target {
                return true;
            }

            let curr_ref = unsafe { curr.deref(pool) };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }
}

impl<T: Clone> TreiberStack<T> {
    fn pop<C: PopType<T>>(
        &self,
        client: &mut C,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<Option<T>, TryFail> {
        let target = client.target().load(Ordering::SeqCst, guard);

        if target.tag() & TryPop::<T>::EMPTY == TryPop::<T>::EMPTY {
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

            if !client.is_try() || target.tag() & TryPop::<T>::RECOVERY == TryPop::<T>::RECOVERY {
                // 복구 로직 실행 조건: "try가 아님" 혹은 "try인데 복구중"

                if client.is_try() {
                    client
                        .target()
                        .store(target.with_tag(TryPop::<T>::DEFAULT), Ordering::SeqCst);
                    // 복구 해제
                    // 복구와 관련된 것이므로 persist 필요 없음
                }

                // target이 stack에서 pop되긴 했는지 확인
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

    impl TestRootObj for TreiberStack<usize> {}

    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn push_pop() {
        const FILE_NAME: &str = "treiber_push_pop.pool";
        run_test::<TreiberStack<usize>, PushPop<TreiberStack<usize>, NR_THREAD, COUNT>, _>(
            FILE_NAME,
            FILE_SIZE,
            NR_THREAD + 1,
        )
    }
}
