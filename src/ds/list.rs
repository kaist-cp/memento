//! Persistent Harris List

use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, DetectableCASAtomic, Handle};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use etrace::some_or;
use std::cmp::Ordering::{Equal, Greater, Less};

use crate::pepoch::{Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*, AsPPtr, PPtr};
use crate::*;

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

#[derive(Debug)]
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

impl<K, V: Collectable> Collectable for Harris<K, V> {
    fn filter(harris: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut harris.result, tid, gc, pool);
    }
}

#[derive(Debug, Default, Memento)]
struct Help {
    cas: Cas,
}

impl Collectable for Help {
    fn filter(help: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Cas::filter(&mut help.cas, tid, gc, pool);
    }
}

#[derive(Debug, Memento)]
struct TryFind<K, V: Collectable> {
    found: Checkpoint<(
        bool,
        PPtr<DetectableCASAtomic<Node<K, V>>>,
        PAtomic<Node<K, V>>,
        PAtomic<Node<K, V>>,
    )>,
    help: Help,
}

impl<K, V: Collectable> Default for TryFind<K, V> {
    fn default() -> Self {
        Self {
            found: Default::default(),
            help: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for TryFind<K, V> {
    fn filter(try_find: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut try_find.found, tid, gc, pool);
        Help::filter(&mut try_find.help, tid, gc, pool);
    }
}

#[derive(Debug, Memento)]
struct Find<K, V: Collectable> {
    try_find: TryFind<K, V>,
}

impl<K, V: Collectable> Default for Find<K, V> {
    fn default() -> Self {
        Self {
            try_find: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Find<K, V> {
    fn filter(find: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        TryFind::filter(&mut find.try_find, tid, gc, pool);
    }
}

/// Lookup memento
#[derive(Debug, Memento)]
pub struct Lookup<K, V: Collectable> {
    find: Find<K, V>,
}

impl<K, V: Collectable> Default for Lookup<K, V> {
    fn default() -> Self {
        Self {
            find: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Lookup<K, V> {
    fn filter(lookup: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Find::filter(&mut lookup.find, tid, gc, pool);
    }
}

#[derive(Debug, Memento)]
struct TryInsert<K, V: Collectable> {
    found: Checkpoint<(
        bool,
        PPtr<DetectableCASAtomic<Node<K, V>>>,
        PAtomic<Node<K, V>>,
        PAtomic<Node<K, V>>,
    )>,
    help: Help,
    insert: Cas,
}

impl<K, V: Collectable> Default for TryInsert<K, V> {
    fn default() -> Self {
        Self {
            found: Default::default(),
            help: Default::default(),
            insert: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for TryInsert<K, V> {
    fn filter(try_ins: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut try_ins.found, tid, gc, pool);
        Help::filter(&mut try_ins.help, tid, gc, pool);
        Cas::filter(&mut try_ins.insert, tid, gc, pool);
    }
}

/// Insert memento
#[derive(Debug, Memento)]
pub struct Insert<K, V: Collectable> {
    node: Checkpoint<PAtomic<Node<K, V>>>,
    find: Find<K, V>,
    try_ins: TryInsert<K, V>,
}

impl<K, V: Collectable> Default for Insert<K, V> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            find: Default::default(),
            try_ins: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Insert<K, V> {
    fn filter(ins: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut ins.node, tid, gc, pool);
        Find::filter(&mut ins.find, tid, gc, pool);
        TryInsert::filter(&mut ins.try_ins, tid, gc, pool);
    }
}

#[derive(Debug, Memento)]
struct TryDelete<K, V: Collectable> {
    find: Find<K, V>,
    next: Checkpoint<PAtomic<Node<K, V>>>,
    logical: Cas,
    physical: Cas,
}

impl<K, V: Collectable> Default for TryDelete<K, V> {
    fn default() -> Self {
        Self {
            find: Default::default(),
            next: Default::default(),
            logical: Default::default(),
            physical: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for TryDelete<K, V> {
    fn filter(try_del: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Find::filter(&mut try_del.find, tid, gc, pool);
        Checkpoint::filter(&mut try_del.next, tid, gc, pool);
        Cas::filter(&mut try_del.logical, tid, gc, pool);
        Cas::filter(&mut try_del.physical, tid, gc, pool);
    }
}

/// Delete memento
#[derive(Debug, Memento)]
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

impl<K, V: Collectable> Collectable for Delete<K, V> {
    fn filter(del: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        TryDelete::filter(&mut del.try_del, tid, gc, pool);
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
#[derive(Debug)]
pub struct List<K, V: Collectable> {
    head: CachePadded<DetectableCASAtomic<Node<K, V>>>,
}

impl<K, V: Collectable> PDefault for List<K, V> {
    fn pdefault(_: &PoolHandle) -> Self {
        Self {
            head: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for List<K, V> {
    fn filter(queue: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        DetectableCASAtomic::filter(&mut queue.head, tid, gc, pool);
    }
}

impl<K: Ord, V: Collectable> List<K, V> {
    fn find_inner<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (
        bool,
        &'g DetectableCASAtomic<Node<K, V>>,
        PShared<'g, Node<K, V>>,
        PShared<'g, Node<K, V>>,
    ) {
        let mut prev = &*self.head;
        let mut curr = self.head.load(Ordering::SeqCst, guard, pool);
        let mut prev_next = curr;

        let found = loop {
            let curr_node = some_or!(unsafe { curr.as_ref(pool) }, break false);
            let next = curr_node.next.load(Ordering::Acquire, guard, pool);

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
        help: &mut Help,
        handle: &Handle,
    ) -> Result<(), ()> {
        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == curr {
            return Ok(());
        }

        // cleanup marked nodes between prev and curr
        prev.cas(prev_next, curr, &mut help.cas, handle)
            .map_err(|_| ())?;

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while node.with_tag(0) != curr {
            unsafe {
                let next = node.deref(handle.pool).next.load(
                    Ordering::Acquire,
                    &handle.guard,
                    handle.pool,
                );
                handle.guard.defer_pdestroy(node);
                node = next;
            }
        }

        Ok(())
    }

    fn try_find<'g>(
        &self,
        key: &K,
        try_find: &mut TryFind<K, V>,
        handle: &'g Handle,
    ) -> Result<
        (
            bool,
            &'g DetectableCASAtomic<Node<K, V>>,
            PShared<'g, Node<K, V>>,
        ),
        (),
    > {
        let (guard, pool) = (&handle.guard, handle.pool);
        let chk = try_find.found.checkpoint(
            || {
                let (found, prev, prev_next, curr) = self.find_inner(key, guard, pool);
                (
                    found,
                    unsafe { prev.as_pptr(pool) },
                    PAtomic::from(prev_next),
                    PAtomic::from(curr),
                )
            },
            handle,
        );
        let (found, prev, prev_next, curr) = (
            chk.0,
            unsafe { chk.1.deref(pool) },
            chk.2.load(Ordering::Relaxed, guard),
            chk.3.load(Ordering::Relaxed, guard),
        );

        self.help(prev, prev_next, curr, &mut try_find.help, handle)
            .map(|_| (found, prev, curr))
    }

    fn find<'g>(
        &self,
        key: &K,
        find: &mut Find<K, V>,
        handle: &'g Handle,
    ) -> (
        bool,
        &'g DetectableCASAtomic<Node<K, V>>,
        PShared<'g, Node<K, V>>,
    ) {
        loop {
            if let Ok(res) = self.try_find(key, &mut find.try_find, handle) {
                return res;
            }
        }
    }

    /// Lookup
    pub fn lookup<'g>(
        &self,
        key: &'g K,
        look: &mut Lookup<K, V>,
        handle: &'g Handle,
    ) -> Option<&'g V> {
        let (found, _, curr) = self.find(key, &mut look.find, handle);
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
        let chk = try_ins.found.checkpoint(
            || {
                let (found, prev, prev_next, curr) = self.find_inner(key, guard, pool);
                if !found {
                    let node_ref = unsafe { node.deref(pool) };
                    // TODO: check if same & otherwise store/flush
                    node_ref.next.inner.store(curr, Ordering::Relaxed);
                    persist_obj(unsafe { &node.deref(pool).next }, true);
                }

                (
                    found,
                    unsafe { prev.as_pptr(pool) },
                    PAtomic::from(prev_next),
                    PAtomic::from(curr),
                )
            },
            handle,
        );
        let (found, prev, prev_next, curr) = (
            chk.0,
            unsafe { chk.1.deref(pool) },
            chk.2.load(Ordering::Relaxed, guard),
            chk.3.load(Ordering::Relaxed, guard),
        );

        self.help(prev, prev_next, curr, &mut try_ins.help, handle)
            .map_err(|_| ListErr::Retry)?;

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
        let (found, prev, curr) = self.find(key, &mut try_del.find, handle);
        if !found {
            return Err(ListErr::Fail);
        }

        let curr_ref = unsafe { curr.deref(handle.pool) };

        // FAO-like..
        let mut next = try_del
            .next
            .checkpoint(
                || {
                    let next = curr_ref
                        .next
                        .load(Ordering::SeqCst, &handle.guard, handle.pool);
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{ploc::Handle, pmem::ralloc::Collectable, test_utils::tests::*};

    const NR_THREAD: usize = 2;
    const NR_COUNT: usize = 10_000;

    struct InsDelLook {
        inserts: [Insert<TestValue, TestValue>; NR_COUNT],
        ins_lookups: [Lookup<TestValue, TestValue>; NR_COUNT],
        deletes: [Delete<TestValue, TestValue>; NR_COUNT],
        del_lookups: [Lookup<TestValue, TestValue>; NR_COUNT],
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
                Insert::filter(&mut m.inserts[i], tid, gc, pool);
                Lookup::filter(&mut m.ins_lookups[i], tid, gc, pool);
                Delete::filter(&mut m.deletes[i], tid, gc, pool);
                Lookup::filter(&mut m.del_lookups[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<InsDelLook> for TestRootObj<List<TestValue, TestValue>> {
        // TODO: Change test
        fn run(&self, mmt: &mut InsDelLook, handle: &Handle) {
            let tid = handle.tid;
            let testee = unsafe { TESTER.as_ref().unwrap().testee(tid, true) };

            for seq in 0..NR_COUNT {
                let key = TestValue::new(tid, seq);

                // insert and lookup
                assert!(self
                    .obj
                    .insert(key, key, &mut mmt.inserts[seq], handle)
                    .is_ok());
                let res = self.obj.lookup(&key, &mut mmt.ins_lookups[seq], handle);

                assert!(res.is_some(), "tid:{tid}, seq:{seq}");
                testee.report(seq, *res.unwrap());

                // delete and lookup
                assert!(self.obj.delete(&key, &mut mmt.deletes[seq], handle).is_ok());
                let res = self.obj.lookup(&key, &mut mmt.del_lookups[seq], handle);

                assert!(res.is_none(), "tid:{tid}, seq:{seq}");
            }
        }
    }

    #[test]
    fn ins_del_look() {
        const FILE_NAME: &str = "list";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<List<TestValue, TestValue>>, InsDelLook>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }
}
