//! Persistent Harris List

use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, DetectableCASAtomic, Handle};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use etrace::some_or;
use std::cmp::Ordering::{Equal, Greater, Less};

use crate::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use crate::pmem::alloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*, AsPPtr, PPtr};
use crate::*;
use mmt_derive::Collectable;

/// Node
#[derive(Debug)]
#[repr(align(128))]
pub struct Node<K, V: Collectable> {
    key: K,
    value: V,
    next: DetectableCASAtomic<Self>,
}

impl<K, V: Collectable> From<(K, V)> for Node<K, V> {
    fn from((key, value): (K, V)) -> Self {
        Self {
            key,
            value,
            next: DetectableCASAtomic::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Node<K, V> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        V::filter(&mut node.value, tid, gc, pool);
        DetectableCASAtomic::filter(&mut node.next, tid, gc, pool);
    }
}

#[derive(Debug, Collectable)]
struct Harris<K, V: Collectable> {
    result: Checkpoint<(
        bool,
        PPtr<DetectableCASAtomic<Node<K, V>>>,
        PAtomic<Node<K, V>>,
        PAtomic<Node<K, V>>,
    )>,
}

impl<K, V: Collectable> Default for Harris<K, V> {
    fn default() -> Self {
        Self {
            result: Default::default(),
        }
    }
}

#[derive(Debug, Memento, Collectable)]
struct TryInsert<K, V: Collectable> {
    found: Checkpoint<(
        bool,
        PPtr<DetectableCASAtomic<Node<K, V>>>,
        PAtomic<Node<K, V>>,
    )>,
    insert: Cas<Node<K, V>>,
}

impl<K, V: Collectable> Default for TryInsert<K, V> {
    fn default() -> Self {
        Self {
            found: Default::default(),
            insert: Default::default(),
        }
    }
}

/// Insert memento
#[derive(Debug, Memento, Collectable)]
pub struct Insert<K, V: Collectable> {
    node: Checkpoint<PAtomic<Node<K, V>>>,
    try_ins: TryInsert<K, V>,
}

impl<K, V: Collectable> Default for Insert<K, V> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_ins: Default::default(),
        }
    }
}

#[derive(Debug, Memento, Collectable)]
struct TryDelete<K, V: Collectable> {
    found: Checkpoint<(
        bool,
        PPtr<DetectableCASAtomic<Node<K, V>>>,
        PAtomic<Node<K, V>>,
    )>,
    next: Checkpoint<PAtomic<Node<K, V>>>,
    logical: Cas<Node<K, V>>,
    physical: Cas<Node<K, V>>,
}

impl<K, V: Collectable> Default for TryDelete<K, V> {
    fn default() -> Self {
        Self {
            found: Default::default(),
            next: Default::default(),
            logical: Default::default(),
            physical: Default::default(),
        }
    }
}

/// Delete memento
#[derive(Debug, Memento, Collectable)]
pub struct Delete<K, V: Collectable> {
    try_del: TryDelete<K, V>,
}

impl<K, V: Collectable> Default for Delete<K, V> {
    fn default() -> Self {
        Self {
            try_del: Default::default(),
        }
    }
}

enum ListErr {
    Retry,
    Fail,
}

/// Insertion error
#[derive(Debug)]
pub struct KeyExists;

/// Deletion error
#[derive(Debug)]
pub struct NotFound;

/// List
#[derive(Debug, Collectable)]
pub struct List<K, V: Collectable> {
    head: CachePadded<DetectableCASAtomic<Node<K, V>>>,
}

impl<K, V: Collectable> PDefault for List<K, V> {
    fn pdefault(_: &Handle) -> Self {
        Self {
            head: Default::default(),
        }
    }
}

impl<K: Ord, V: Collectable> List<K, V> {
    fn find_inner<'g>(
        &'g self,
        key: &K,
        handle: &'g Handle,
    ) -> (
        bool,
        &'g DetectableCASAtomic<Node<K, V>>,
        PShared<'g, Node<K, V>>,
        PShared<'g, Node<K, V>>,
    ) {
        let mut prev = &*self.head;
        let mut curr = self.head.load(Ordering::SeqCst, handle);
        let mut prev_next = curr;

        let found = loop {
            let curr_node = some_or!(unsafe { curr.as_ref(handle.pool) }, break false);
            let next = curr_node.next.load(Ordering::Acquire, handle);

            if next.tag() != 0 {
                curr = next.with_tag(0);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    curr = next.with_tag(0);
                    prev = &curr_node.next;
                    prev_next = next;
                }
                Equal => break true,
                Greater => break false,
            }
        };

        (found, prev, prev_next, curr)
    }

    fn help<'g>(
        &self,
        prev: &'g DetectableCASAtomic<Node<K, V>>,
        prev_next: PShared<'g, Node<K, V>>,
        curr: PShared<'g, Node<K, V>>,
        handle: &Handle,
    ) -> Result<(), ()> {
        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == curr {
            return Ok(());
        }

        // cleanup marked nodes between prev and curr
        prev.cas_non_detectable(prev_next, curr, handle)
            .map_err(|_| ())?;

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while node.with_tag(0) != curr {
            unsafe {
                let next = node.deref(handle.pool).next.load(Ordering::Acquire, handle);
                handle.guard.defer_pdestroy(node);
                node = next;
            }
        }

        Ok(())
    }

    fn find<'g>(
        &'g self,
        key: &K,
        handle: &'g Handle,
    ) -> (
        bool,
        &'g DetectableCASAtomic<Node<K, V>>,
        PShared<'g, Node<K, V>>,
    ) {
        loop {
            let (found, prev, prev_next, curr) = self.find_inner(key, handle);
            if self.help(prev, prev_next, curr, handle).is_ok() {
                return (found, prev, curr);
            }
        }
    }

    /// Lookup
    pub fn lookup<'g>(&'g self, key: &'g K, handle: &'g Handle) -> Option<&'g V> {
        let (found, _, curr) = self.find(key, handle);
        if found {
            unsafe { curr.as_ref(handle.pool).map(|n| &n.value) }
        } else {
            None
        }
    }

    fn try_insert(
        &self,
        node: PShared<'_, Node<K, V>>,
        key: &K,
        try_ins: &mut TryInsert<K, V>,
        handle: &Handle,
    ) -> Result<(), ListErr> {
        let (guard, pool) = (&handle.guard, handle.pool);

        let (found, prev, curr) = {
            let chk = try_ins.found.checkpoint(
                || {
                    let (found, prev, curr) = self.find(key, handle);

                    if !found {
                        let node_ref = unsafe { node.deref(pool) };
                        // TODO: check if same & otherwise store/flush
                        node_ref.next.inner.store(curr, Ordering::Relaxed);
                        persist_obj(unsafe { &node.deref(pool).next }, true);
                    }

                    (found, unsafe { prev.as_pptr(pool) }, PAtomic::from(curr))
                },
                handle,
            );
            (
                chk.0,
                unsafe { chk.1.deref(pool) },
                chk.2.load(Ordering::Relaxed, guard),
            )
        };

        if found {
            unsafe { guard.defer_pdestroy(node) };
            return Err(ListErr::Fail);
        }

        prev.cas(curr, node, &mut try_ins.insert, handle)
            .map(|_| ())
            .map_err(|_| ListErr::Retry)
    }

    /// Insert
    pub fn insert(
        &self,
        key: K,
        value: V,
        ins: &mut Insert<K, V>,
        handle: &Handle,
    ) -> Result<(), KeyExists> {
        let (guard, pool) = (&handle.guard, handle.pool);
        let node = ins
            .node
            .checkpoint(
                || {
                    let node = POwned::new(Node::from((key, value)), pool);
                    persist_obj(unsafe { node.deref(pool) }, true);
                    PAtomic::from(node)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);
        let node_ref = unsafe { node.deref(pool) };

        loop {
            match self.try_insert(node, &node_ref.key, &mut ins.try_ins, handle) {
                Ok(()) => return Ok(()),
                Err(ListErr::Fail) => return Err(KeyExists),
                Err(ListErr::Retry) => (),
            };
        }
    }

    fn try_delete(
        &self,
        key: &K,
        try_del: &mut TryDelete<K, V>,
        handle: &Handle,
    ) -> Result<(), ListErr> {
        let (guard, pool) = (&handle.guard, handle.pool);

        let (found, prev, curr) = {
            let chk = try_del.found.checkpoint(
                || {
                    let (found, prev, curr) = self.find(key, handle);
                    (found, unsafe { prev.as_pptr(pool) }, PAtomic::from(curr))
                },
                handle,
            );
            (
                chk.0,
                unsafe { chk.1.deref(pool) },
                chk.2.load(Ordering::Relaxed, guard),
            )
        };

        if !found {
            return Err(ListErr::Fail);
        }

        let curr_ref = unsafe { curr.deref(handle.pool) };

        // FAO-like..
        let mut next = try_del
            .next
            .checkpoint(
                || {
                    let next = curr_ref.next.load(Ordering::SeqCst, handle);
                    PAtomic::from(next)
                },
                handle,
            )
            .load(Ordering::Relaxed, &handle.guard);
        if next.tag() == 1 {
            return Err(ListErr::Retry);
        }
        let mut res = curr_ref
            .next
            .cas(next, next.with_tag(1), &mut try_del.logical, handle);

        while let Err(e) = res {
            next = try_del
                .next
                .checkpoint(|| PAtomic::from(e), handle)
                .load(Ordering::Relaxed, &handle.guard);
            if next.tag() == 1 {
                return Err(ListErr::Retry);
            }
            res = curr_ref
                .next
                .cas(next, next.with_tag(1), &mut try_del.logical, handle)
        }

        if prev.cas(curr, next, &mut try_del.physical, handle).is_ok() {
            unsafe { handle.guard.defer_pdestroy(curr) };
        }

        Ok(())
    }

    /// Delete
    pub fn delete(&self, key: &K, del: &mut Delete<K, V>, handle: &Handle) -> Result<(), NotFound> {
        loop {
            match self.try_delete(key, &mut del.try_del, handle) {
                Ok(()) => return Ok(()),
                Err(ListErr::Fail) => return Err(NotFound),
                Err(ListErr::Retry) => (),
            };
        }
    }
}

#[allow(dead_code)]
pub(crate) mod test {
    use test_utils::distributer::*;

    use super::*;
    use crate::{ploc::Handle, pmem::alloc::Collectable, test_utils::tests::*};

    const NR_THREAD: usize = 2;
    #[cfg(not(feature = "pmcheck"))]
    const NR_COUNT: usize = 10_000;
    #[cfg(feature = "pmcheck")]
    const NR_COUNT: usize = 10;

    const PADDED: usize = NR_THREAD + 1;

    lazy_static::lazy_static! {
        static ref ITEMS: Distributer<PADDED, NR_COUNT> = Distributer::new();
    }

    struct InsDelLook {
        inserts: [Insert<TestValue, TestValue>; NR_COUNT],
        ins_lookups: [Checkpoint<Option<TestValue>>; NR_COUNT],
        deletes: [Delete<TestValue, TestValue>; NR_COUNT],
        del_lookups: [Checkpoint<Option<TestValue>>; NR_COUNT],
    }

    impl Memento for InsDelLook {
        fn clear(&mut self) {
            for i in 0..NR_COUNT {
                self.inserts[i].clear();
                self.ins_lookups[i].clear();
                self.deletes[i].clear();
                self.del_lookups[i].clear();
            }
        }
    }

    impl Default for InsDelLook {
        fn default() -> Self {
            Self {
                inserts: array_init::array_init(|_| Default::default()),
                ins_lookups: array_init::array_init(|_| Default::default()),
                deletes: array_init::array_init(|_| Default::default()),
                del_lookups: array_init::array_init(|_| Default::default()),
            }
        }
    }

    impl Collectable for InsDelLook {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..NR_COUNT {
                Collectable::filter(&mut m.inserts[i], tid, gc, pool);
                Collectable::filter(&mut m.ins_lookups[i], tid, gc, pool);
                Collectable::filter(&mut m.deletes[i], tid, gc, pool);
                Collectable::filter(&mut m.del_lookups[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<InsDelLook> for TestRootObj<List<TestValue, TestValue>> {
        fn run(&self, mmt: &mut InsDelLook, handle: &Handle) {
            let tid = handle.tid;
            let testee = unsafe { TESTER.as_ref().unwrap().testee(true, handle) };

            for seq in 0..NR_COUNT {
                let key = TestValue::new(tid, seq);

                // insert
                assert!(self
                    .obj
                    .insert(key, key, &mut mmt.inserts[seq], handle)
                    .is_ok());

                // make it can be removed by other thread
                let _ = ITEMS.produce(tid, seq);

                // decide key to delete
                let (t_producer, _) = ITEMS.consume(tid, seq).unwrap();
                let key_delete = TestValue::new(t_producer, seq);

                // lookup before delete
                let res = mmt.ins_lookups[seq].checkpoint(
                    || {
                        self.obj
                            .lookup(&key_delete, handle)
                            .map_or(None, |v| Some(*v))
                    },
                    handle,
                );
                assert!(
                    res.is_some(),
                    "tid:{tid}, seq:{seq}, remove{:?}",
                    key_delete
                );

                // delete
                assert!(self
                    .obj
                    .delete(&key_delete, &mut mmt.deletes[seq], handle)
                    .is_ok());

                // lookup after delete
                let res = mmt.del_lookups[seq].checkpoint(
                    || {
                        self.obj
                            .lookup(&key_delete, handle)
                            .map_or(None, |v| Some(*v))
                    },
                    handle,
                );
                assert!(
                    res.is_none(),
                    "tid:{tid}, seq:{seq}, remove:{:?}",
                    key_delete
                );
                testee.report(seq, key_delete);
            }
        }
    }

    #[test]
    fn ins_del_look() {
        const FILE_NAME: &str = "list";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        lazy_static::initialize(&ITEMS);

        run_test::<TestRootObj<List<TestValue, TestValue>>, InsDelLook>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }

    /// Test function for pmcheck
    #[cfg(feature = "pmcheck")]
    pub(crate) fn pmcheck_ins_del_look() {
        const FILE_NAME: &str = "list";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        lazy_static::initialize(&ITEMS);

        run_test::<TestRootObj<List<TestValue, TestValue>>, InsDelLook>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }
}
