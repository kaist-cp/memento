//! Persistent list

use std::sync::atomic::{AtomicUsize, Ordering};

use etrace::some_or;

use crate::{
    pepoch::{self as epoch, atomic::Pointer, Guard, PAtomic, PDestroyable, POwned, PShared},
    persistent::Memento,
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        AsPPtr, PoolHandle,
    },
};

/// List에 들어가는 node
// TODO: V가 포인터일 수 있으니 V도 Collectable이여야함
#[derive(Debug)]
pub struct Node<K, V> {
    key: K,
    value: V,
    next: PAtomic<Node<K, V>>,
    remover: AtomicUsize,
    inserted: bool,
}

impl<K, V> Node<K, V>
where
    K: Eq,
{
    /// Creates a new node.
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            next: PAtomic::null(),
            remover: AtomicUsize::new(List::<K, V>::no_remover()),
            inserted: false,
        }
    }
}

impl<K, V> Collectable for Node<K, V> {
    fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut next = node.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            let next_ref = unsafe { next.deref_mut(pool) };
            Node::mark(next_ref, gc);
        }
    }
}

/// List의 제일 앞에 element를 추가하는 Memento
#[derive(Debug)]
pub struct InsertFront<K, V> {
    node: PAtomic<Node<K, V>>,
}

impl<K, V> Default for InsertFront<K, V> {
    fn default() -> Self {
        Self {
            node: PAtomic::null(),
        }
    }
}

impl<K: 'static, V: 'static> Collectable for InsertFront<K, V> {
    fn filter(insf: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut node = insf.node.load(Ordering::SeqCst, guard);
        if !node.is_null() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::mark(node_ref, gc);
        }
    }
}

impl<K: 'static, V: 'static> Memento for InsertFront<K, V>
where
    K: Eq,
{
    type Object<'o> = &'o List<K, V>;
    type Input = (K, V);
    type Output<'o> = ();
    type Error = !;

    fn run<'o>(
        &'o mut self,
        list: Self::Object<'o>,
        (key, value): Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        list.insert_front(self, key, value, guard, pool);
        Ok(())
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, _pool: &'static PoolHandle) {
        let node = self.node.load(Ordering::SeqCst, guard);
        if !node.is_null() {
            self.node.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.node, true);
        }
    }
}

/// List에서 key에 해당하는 element를 제거하는 Memento
#[derive(Debug)]
pub struct Remove<K, V> {
    target: PAtomic<Node<K, V>>,
}

impl<K, V> Default for Remove<K, V> {
    fn default() -> Self {
        Self {
            target: PAtomic::null(),
        }
    }
}

impl<K, V> Remove<K, V> {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }
}

impl<K: 'static, V: 'static> Collectable for Remove<K, V> {
    fn filter(rmv: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut target = rmv.target.load(Ordering::SeqCst, guard);
        if !target.is_null() {
            let target_ref = unsafe { target.deref_mut(pool) };
            Node::mark(target_ref, gc);
        }
    }
}

impl<K: 'static, V: 'static> Memento for Remove<K, V>
where
    K: Eq,
{
    type Object<'o> = &'o List<K, V>;
    type Input = K;
    type Output<'o> = bool; // TODO: PoolHandle에 관한 디자인 합의 이후 Option<&'g V>로 바꾸기 (lifetime issue)
    type Error = !;

    fn run<'o>(
        &'o mut self,
        list: Self::Object<'o>,
        key: Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        Ok(list.remove(self, &key, guard, pool))
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        let target = self.target.load(Ordering::SeqCst, guard);
        if !target.is_null() {
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);

            if unsafe { target.deref(pool) }.remover.load(Ordering::SeqCst) == self.id(pool) {
                unsafe { guard.defer_pdestroy(target) };
            }
        }
    }
}

/// List 탐색 도중 동시적으로 node 제거가 생길 경우 발생하는 에러
#[derive(Debug)]
pub struct Deprecated;

/// List의 특정 node를 가리키는 cursor (Volatile)
#[derive(Debug)]
pub struct Cursor<'g, K, V> {
    prev: &'g PAtomic<Node<K, V>>,

    /// cursor가 가리키는 현재 node
    pub curr: PShared<'g, Node<K, V>>,
}

impl<'g, K, V> Clone for Cursor<'g, K, V> {
    fn clone(&self) -> Self {
        Self {
            prev: self.prev,
            curr: self.curr,
        }
    }
}

impl<'g, K, V> Cursor<'g, K, V>
where
    K: Eq,
{
    const NOT_DELETED: usize = 0;
    const DELETED: usize = 1;

    /// Returns the current node.
    pub fn curr(&self) -> PShared<'g, Node<K, V>> {
        self.curr
    }

    /// Based on Harris-Michael.
    #[inline]
    fn find(&mut self, key: &K, guard: &'g Guard, pool: &'g PoolHandle) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(self.curr.tag(), Self::NOT_DELETED);

            let curr_node = some_or!(unsafe { self.curr.as_ref(pool) }, return Ok(false));
            let mut next = curr_node.next.load(Ordering::SeqCst, guard);

            if next.tag() != Self::NOT_DELETED {
                next = next.with_tag(Self::NOT_DELETED);
                let res = self.prev.compare_exchange(
                    self.curr,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
                persist_obj(self.prev, true); // TODO: CAS 실패한 애도 persist 해야하는지 고민

                if res.is_err() {
                    return Err(());
                }

                self.curr = next;
                continue;
            }

            if curr_node.key != *key {
                self.prev = &curr_node.next;
                self.curr = next;
            } else {
                return Ok(true);
            }
        }
    }

    /// Lookups the value.
    #[inline]
    pub fn lookup(&self, pool: &'g PoolHandle) -> Option<(&'g K, &'g V)> {
        unsafe { self.curr.as_ref(pool).map(|n| (&n.key, &n.value)) }
    }

    /// Inserts a value.
    #[inline]
    fn insert(
        &mut self,
        node: PShared<'g, Node<K, V>>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let node_ref = unsafe { node.deref(pool) };
        node_ref.next.store(self.curr, Ordering::SeqCst);

        let res =
            self.prev
                .compare_exchange(self.curr, node, Ordering::SeqCst, Ordering::SeqCst, guard);
        persist_obj(self.prev, true); // TODO: CAS 실패한 애도 persist 해야하는지 고민

        res.map(|_| {
            self.curr = node;
        })
        .map_err(|_| ())
    }

    /// Deletes the current node.
    #[inline]
    fn remove(
        self,
        client: &Remove<K, V>,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<&'g V, ()> {
        // 우선 내가 지우려는 node를 가리키고
        client.target.store(self.curr, Ordering::SeqCst);

        // logical remove를 시행
        let curr_node = unsafe { self.curr.as_ref(pool) }.unwrap();
        let next = curr_node
            .next
            .fetch_or(Self::DELETED, Ordering::SeqCst, guard);
        persist_obj(&curr_node.next, true); // TODO: FAO 실패한 애도 persist 해야하는지 고민

        if next.tag() == Self::DELETED {
            return Err(());
        }

        // 지워지는 node에 내 이름 새겨넣음
        // CAS인 이유: remove 복구 중인 스레드와 경합이 일어날 수 있음
        if curr_node
            .remover
            .compare_exchange(
                List::<K, V>::no_remover(),
                client.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err(());
        }

        persist_obj(&curr_node.remover, true);

        let _ =
            self.prev
                .compare_exchange(self.curr, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        persist_obj(&self.prev, true);

        Ok(&curr_node.value)
    }

    /// cursor를 다음 node로 옮김
    #[inline]
    pub fn next(
        &mut self,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<Option<(&K, &V)>, Deprecated> {
        debug_assert_eq!(self.curr.tag(), Self::NOT_DELETED);

        let curr_node = some_or!(unsafe { self.curr.as_ref(pool) }, return Ok(None));
        let next = curr_node.next.load(Ordering::SeqCst, guard);

        self.prev = &curr_node.next;
        self.curr = next;

        loop {
            let curr_node = some_or!(unsafe { self.curr.as_ref(pool) }, return Ok(None));
            let mut next = curr_node.next.load(Ordering::SeqCst, guard);

            if next.tag() != Self::DELETED {
                return Ok(Some((&curr_node.key, &curr_node.value)));
            }

            next = next.with_tag(Self::NOT_DELETED);
            if self
                .prev
                .compare_exchange(self.curr, next, Ordering::SeqCst, Ordering::SeqCst, guard)
                .is_err()
            {
                return Err(Deprecated);
            }
            self.curr = next;
        }
    }
}

/// Ticket lock에서 쓰기 위한 list
/// `InsertFront`로 주어지는 Key는 모두 unique함을 가정
#[derive(Debug)]
pub struct List<K, V>
where
    K: Eq,
{
    head: PAtomic<Node<K, V>>,
}

impl<K, V> Default for List<K, V>
where
    K: Eq,
{
    fn default() -> Self {
        Self {
            head: PAtomic::null(),
        }
    }
}

impl<K, V> List<K, V>
where
    K: Eq,
{
    /// List의 head node
    pub fn head<'g>(&'g self, guard: &'g Guard) -> Cursor<'g, K, V> {
        Cursor {
            prev: &self.head,
            curr: self.head.load(Ordering::SeqCst, guard),
        }
    }

    /// Finds a key using the given find strategy.
    #[inline]
    pub fn find<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (bool, Cursor<'g, K, V>) {
        loop {
            let mut cursor = self.head(guard);
            if let Ok(r) = cursor.find(key, guard, pool) {
                return (r, cursor);
            }
        }
    }

    /// List에서 주어진 `key`를 가지는 element의 값을 구함
    #[inline]
    pub fn lookup<'g>(&'g self, key: &K, guard: &'g Guard, pool: &'g PoolHandle) -> Option<&'g V> {
        let (found, cursor) = self.find(key, guard, pool);
        if found {
            cursor.lookup(pool).map(|(_, v)| v)
        } else {
            None
        }
    }

    #[inline]
    fn insert_front<'g>(
        &'g self,
        client: &InsertFront<K, V>,
        key: K,
        value: V,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) {
        let mut node = client.node.load(Ordering::SeqCst, guard);

        if node.is_null() {
            // node가 null이면 할당이 안 된 것
            let n = POwned::new(Node::new(key, value), pool).into_shared(guard);

            client.node.store(n, Ordering::SeqCst);
            node = n;
        } else if self.search_node(node, guard, pool) || unsafe { node.deref(pool).inserted } {
            return; // 이미 예전에 삽입함
        }

        loop {
            // unique key만 주어진다는 보장
            if self.head(guard).insert(node, guard, pool).is_ok() {
                return; // 삽입 성공
            }
        }
    }

    #[inline]
    fn remove<'g>(
        &'g self,
        client: &Remove<K, V>,
        key: &K,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> bool {
        // TODO: PoolHandle에 관한 디자인 합의 이후 Option<&'g V>로 바꾸기 (lifetime issue)
        const NOT_FOUND: usize = 1;
        let target = client.target.load(Ordering::SeqCst, guard);

        if target.tag() == NOT_FOUND {
            // post-crash execution (empty)
            return false;
        }

        if !target.is_null() {
            // post-crash execution (trying)
            let target_ref = unsafe { target.deref(pool) };
            let remover = target_ref.remover.load(Ordering::SeqCst);

            // target이 내가 pop한 게 맞는지 확인
            if remover == client.id(pool) {
                return true; // Some(&target_ref.value);
            };

            // target이 list에서 remove 되긴 했는지 확인
            if remover == Self::no_remover() && !self.search_node(target, guard, pool) {
                // 누군가가 target을 list에서 logically remove한 뒤, remover를 기록하기 직전에 crash가 남. 그러므로 remover를 마저 기록해줌.
                // CAS인 이유: 서로 누가 진짜 주인인 줄 모르고 모두가 복구하면서 같은 target을 노리고 있을 수 있음
                if target_ref
                    .remover
                    .compare_exchange(
                        Self::no_remover(),
                        client.id(pool),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    persist_obj(&target_ref.remover, true);
                    return true; // Some(&target_ref.value);
                }
            }
        }

        loop {
            let (found, cursor) = self.find(key, guard, pool);
            if !found {
                return false; // None;
            }

            match cursor.remove(client, guard, pool) {
                Err(()) => continue,
                Ok(_) => return true, // Some(value),
            }
        }
    }

    /// `node`가 List 안에 있는지 head부터 끝까지 순회하며 검색
    /// `find()`와의 차이: `find()`는 key를 찾음(public). 이건 특정 node를 찾음(private).
    fn search_node(&self, node: PShared<'_, Node<K, V>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut curr = self.head.load(Ordering::SeqCst, guard);

        while !curr.is_null() {
            if curr == node {
                return true;
            }

            let curr_ref = unsafe { curr.deref(pool) };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }

    #[inline]
    fn no_remover() -> usize {
        let null = PShared::<Remove<K, V>>::null();
        null.into_usize()
    }
}

impl<K, V> Collectable for List<K, V>
where
    K: Eq,
{
    fn filter(list: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut head = list.head.load(Ordering::SeqCst, guard);
        if !head.is_null() {
            let head_ref = unsafe { head.deref_mut(pool) };
            Node::mark(head_ref, gc);
        }
    }
}
