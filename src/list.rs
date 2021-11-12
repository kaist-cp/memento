//! Persistent list

use std::{
    os::raw::c_char,
    sync::atomic::{AtomicUsize, Ordering},
};

use etrace::some_or;

use crate::{
    pepoch::{self as epoch, atomic::Pointer, Guard, PAtomic, POwned, PShared},
    persistent::POp,
    plocation::{
        ralloc::{Collectable, GarbageCollection},
        AsPPtr, PoolHandle,
    },
};

/// TODO: doc
#[derive(Debug)]
pub struct Node<K, V> {
    key: K,
    value: V,
    next: PAtomic<Node<K, V>>,
    remover: AtomicUsize,
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
        }
    }
}

/// TODO: doc
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
    unsafe extern "C" fn filter(ptr: *mut c_char, gc: *mut GarbageCollection) {
        todo!()
    }
}

impl<K: 'static, V: 'static> POp for InsertFront<K, V>
where
    K: Eq,
{
    type Object<'o> = &'o List<K, V>;
    type Input = (K, V);
    type Output<'o> = ();
    type Error = ();

    fn run<'o>(
        &'o mut self,
        list: Self::Object<'o>,
        (key, value): Self::Input,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin(pool);
        list.insert_front(self, key, value, &guard, pool)
    }

    fn reset(&mut self, _: bool) {
        // TODO: if not finished -> free node
        self.node.store(PShared::null(), Ordering::SeqCst);
    }
}

/// TODO: doc
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
    unsafe extern "C" fn filter(ptr: *mut c_char, gc: *mut GarbageCollection) {
        todo!()
    }
}

impl<K: 'static, V: 'static> POp for Remove<K, V>
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
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin(pool);
        Ok(list.remove(self, &key, &guard, pool))
    }

    fn reset(&mut self, nested: bool) {
        let _ = nested;
        unimplemented!()
    }
}

/// TODO: doc
#[derive(Debug)]
pub struct Cursor<'g, K, V> {
    prev: &'g PAtomic<Node<K, V>>,

    /// TODO: doc
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
    fn find(&mut self, key: &K, guard: &'g Guard<'_>, pool: &'g PoolHandle) -> Result<bool, ()> {
        loop {
            debug_assert_eq!(self.curr.tag(), Self::NOT_DELETED);

            let curr_node = some_or!(unsafe { self.curr.as_ref(pool) }, return Ok(false));
            let mut next = curr_node.next.load(Ordering::SeqCst, guard);

            if next.tag() != Self::NOT_DELETED {
                next = next.with_tag(Self::NOT_DELETED);
                let _ = self
                    .prev
                    .compare_exchange(self.curr, next, Ordering::SeqCst, Ordering::SeqCst, guard)
                    .map_err(|_| ())?;
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
        guard: &'g Guard<'_>,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let node_ref = unsafe { node.deref(pool) };
        node_ref.next.store(self.curr, Ordering::SeqCst);

        self.prev
            .compare_exchange(self.curr, node, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| {
                self.curr = node;
            })
            .map_err(|_| ())
    }

    /// Deletes the current node.
    #[inline]
    fn remove(
        self,
        client: &Remove<K, V>,
        guard: &'g Guard<'_>,
        pool: &'g PoolHandle,
    ) -> Result<&'g V, ()> {
        // 우선 내가 지우려는 node를 가리키고
        client.target.store(self.curr, Ordering::SeqCst);

        // logical remove를 시행
        let curr_node = unsafe { self.curr.as_ref(pool) }.unwrap();
        let next = curr_node
            .next
            .fetch_or(Self::DELETED, Ordering::SeqCst, guard);
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

        let _ =
            self.prev
                .compare_exchange(self.curr, next, Ordering::SeqCst, Ordering::SeqCst, guard);

        Ok(&curr_node.value)
    }

    /// TODO: doc
    #[inline]
    pub fn next(
        &mut self,
        guard: &'g Guard<'_>,
        pool: &'g PoolHandle,
    ) -> Result<Option<(&K, &V)>, ()> {
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
            let _ = self
                .prev
                .compare_exchange(self.curr, next, Ordering::SeqCst, Ordering::SeqCst, guard)
                .map_err(|_| ())?;
            self.curr = next;
        }
    }
}

/// TODO: doc
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
    /// TODO: doc
    pub fn head<'g>(&'g self, guard: &'g Guard<'_>) -> Cursor<'g, K, V> {
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
        guard: &'g Guard<'_>,
        pool: &'g PoolHandle,
    ) -> (bool, Cursor<'g, K, V>) {
        loop {
            let mut cursor = self.head(guard);
            if let Ok(r) = cursor.find(key, guard, pool) {
                return (r, cursor);
            }
        }
    }

    /// TODO: doc
    #[inline]
    pub fn lookup<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard<'_>,
        pool: &'g PoolHandle,
    ) -> Option<&'g V> {
        let (found, cursor) = self.find(key, guard, pool);
        if found {
            cursor.lookup(pool).map(|(_, v)| v)
        } else {
            None
        }
    }

    /// TODO: doc
    #[inline]
    fn insert_front<'g>(
        &'g self,
        client: &InsertFront<K, V>,
        key: K,
        value: V,
        guard: &'g Guard<'_>,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let mut node = client.node.load(Ordering::SeqCst, guard);

        if node.is_null() {
            // node가 null이면 할당이 안 된 것
            let n = POwned::new(Node::new(key, value), pool).into_shared(guard);

            client.node.store(n, Ordering::SeqCst);
            node = n;
        }

        let node_ref = unsafe { node.deref(pool) };
        let (found, cursor) = self.find(&node_ref.key, guard, pool);

        if found {
            if node.as_ptr() == cursor.curr().as_ptr() {
                return Ok(()); // 내가 넣은 것
            } else {
                return Err(()); // 이미 같은 키가 존재
            }
        } else if node_ref.remover.load(Ordering::SeqCst) != Self::no_remover() {
            return Ok(()); // 내가 예전에 넣었는데 누군가 뺌
        }

        loop {
            if self.head(guard).insert(node, guard, pool).is_ok() {
                return Ok(()); // 삽입 성공
            }

            let (found, _) = self.find(&node_ref.key, guard, pool);
            if found {
                return Err(()); // 이미 같은 키가 존재
            }
        }
    }

    /// TODO: doc
    #[inline]
    fn remove<'g>(
        &'g self,
        client: &Remove<K, V>,
        key: &K,
        guard: &'g Guard<'_>,
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
    fn search_node(
        &self,
        node: PShared<'_, Node<K, V>>,
        guard: &Guard<'_>,
        pool: &PoolHandle,
    ) -> bool {
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
