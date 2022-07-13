//! Persistent Harris List

use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, DetectableCASAtomic};
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

#[derive(Debug)]
struct Help {
    cas: Cas,
}

impl Default for Help {
    fn default() -> Self {
        Self {
            cas: Default::default(),
        }
    }
}

impl Collectable for Help {
    fn filter(help: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Cas::filter(&mut help.cas, tid, gc, pool);
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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
#[derive(Debug)]
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

#[derive(Debug)]
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
#[derive(Debug)]
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

#[derive(Debug)]
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
#[derive(Debug)]
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

    fn help<'g, const REC: bool>(
        &self,
        prev: &'g DetectableCASAtomic<Node<K, V>>,
        prev_next: PShared<'g, Node<K, V>>,
        curr: PShared<'g, Node<K, V>>,
        help: &mut Help,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<(), ()> {
        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == curr {
            return Ok(());
        }

        // cleanup marked nodes between prev and curr
        let _ = prev
            .cas::<REC>(prev_next, curr, &mut help.cas, tid, guard, pool)
            .map_err(|_| ())?;

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while node.with_tag(0) != curr {
            unsafe {
                let next = node.deref(pool).next.load(Ordering::Acquire, guard, pool);
                guard.defer_pdestroy(node);
                node = next;
            }
        }

        Ok(())
    }

    fn try_find<'g, const REC: bool>(
        &self,
        key: &K,
        try_find: &mut TryFind<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<
        (
            bool,
            &'g DetectableCASAtomic<Node<K, V>>,
            PShared<'g, Node<K, V>>,
        ),
        (),
    > {
        let chk = try_find.found.checkpoint::<REC, _>(
            || {
                let (found, prev, prev_next, curr) = self.find_inner(key, guard, pool);
                (
                    found,
                    unsafe { prev.as_pptr(pool) },
                    PAtomic::from(prev_next),
                    PAtomic::from(curr),
                )
            },
            tid,
            pool,
        );
        let (found, prev, prev_next, curr) = (
            chk.0,
            unsafe { chk.1.deref(pool) },
            chk.2.load(Ordering::Relaxed, guard),
            chk.3.load(Ordering::Relaxed, guard),
        );

        self.help::<REC>(prev, prev_next, curr, &mut try_find.help, tid, guard, pool)
            .map(|_| (found, prev, curr))
    }

    fn find<'g, const REC: bool>(
        &self,
        key: &K,
        find: &mut Find<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (
        bool,
        &'g DetectableCASAtomic<Node<K, V>>,
        PShared<'g, Node<K, V>>,
    ) {
        if let Ok(res) = self.try_find::<REC>(key, &mut find.try_find, tid, guard, pool) {
            return res;
        }

        loop {
            if let Ok(res) = self.try_find::<false>(key, &mut find.try_find, tid, guard, pool) {
                return res;
            }
        }
    }

    /// Lookup
    pub fn lookup<'g, const REC: bool>(
        &self,
        key: &'g K,
        look: &mut Lookup<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Option<&'g V> {
        let (found, _, curr) = self.find::<REC>(key, &mut look.find, tid, guard, pool);
        if found {
            unsafe { curr.as_ref(pool).map(|n| &n.value) }
        } else {
            None
        }
    }

    fn try_insert<const REC: bool>(
        &self,
        node: PShared<'_, Node<K, V>>,
        key: &K,
        try_ins: &mut TryInsert<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ListErr> {
        let chk = try_ins.found.checkpoint::<REC, _>(
            || {
                let (found, prev, prev_next, curr) = self.find_inner(key, guard, pool);
                if !found {
                    let node_ref = unsafe { node.deref(pool) };
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
            tid,
            pool,
        );
        let (found, prev, prev_next, curr) = (
            chk.0,
            unsafe { chk.1.deref(pool) },
            chk.2.load(Ordering::Relaxed, guard),
            chk.3.load(Ordering::Relaxed, guard),
        );

        let _ = self
            .help::<REC>(prev, prev_next, curr, &mut try_ins.help, tid, guard, pool)
            .map_err(|_| ListErr::Retry)?;

        if found {
            unsafe { guard.defer_pdestroy(node) };
            return Err(ListErr::Fail);
        }

        prev.cas::<REC>(curr, node, &mut try_ins.insert, tid, guard, pool)
            .map(|_| ())
            .map_err(|_| ListErr::Retry)
    }

    /// Insert
    pub fn insert<const REC: bool>(
        &self,
        key: K,
        value: V,
        ins: &mut Insert<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let node = ins
            .node
            .checkpoint::<REC, _>(
                || {
                    let node = POwned::new(Node::from((key, value)), pool);
                    persist_obj(unsafe { node.deref(pool) }, true);
                    PAtomic::from(node)
                },
                tid,
                pool,
            )
            .load(Ordering::Relaxed, guard);
        let node_ref = unsafe { node.deref(pool) };

        let () =
            match self.try_insert::<REC>(node, &node_ref.key, &mut ins.try_ins, tid, guard, pool) {
                Ok(()) => return Ok(()),
                Err(ListErr::Fail) => return Err(()),
                Err(ListErr::Retry) => (),
            };

        loop {
            let () = match self.try_insert::<REC>(
                node,
                &node_ref.key,
                &mut ins.try_ins,
                tid,
                guard,
                pool,
            ) {
                Ok(()) => return Ok(()),
                Err(ListErr::Fail) => return Err(()),
                Err(ListErr::Retry) => (),
            };
        }
    }

    fn try_delete<const REC: bool>(
        &self,
        key: &K,
        try_del: &mut TryDelete<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ListErr> {
        let (found, prev, curr) = self.find::<REC>(key, &mut try_del.find, tid, guard, pool);
        if !found {
            return Err(ListErr::Fail);
        }

        let curr_ref = unsafe { curr.deref(pool) };

        // FAO-like..
        let mut next = try_del
            .next
            .checkpoint::<REC, _>(
                || {
                    let next = curr_ref.next.load(Ordering::SeqCst, guard, pool);
                    PAtomic::from(next)
                },
                tid,
                pool,
            )
            .load(Ordering::Relaxed, guard);
        if next.tag() == 1 {
            return Err(ListErr::Retry);
        }
        let mut res = curr_ref.next.cas::<REC>(
            next,
            next.with_tag(1),
            &mut try_del.logical,
            tid,
            guard,
            pool,
        );

        while let Err(e) = res {
            next = try_del
                .next
                .checkpoint::<false, _>(|| PAtomic::from(e), tid, pool)
                .load(Ordering::Relaxed, guard);
            if next.tag() == 1 {
                return Err(ListErr::Retry);
            }
            res = curr_ref.next.cas::<false>(
                next,
                next.with_tag(1),
                &mut try_del.logical,
                tid,
                guard,
                pool,
            )
        }

        if prev
            .cas::<REC>(curr, next, &mut try_del.physical, tid, guard, pool)
            .is_ok()
        {
            unsafe { guard.defer_pdestroy(curr) };
        }

        Ok(())
    }

    /// Delete
    pub fn delete<const REC: bool>(
        &self,
        key: &K,
        del: &mut Delete<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let () = match self.try_delete::<REC>(key, &mut del.try_del, tid, guard, pool) {
            Ok(()) => return Ok(()),
            Err(ListErr::Fail) => return Err(()),
            Err(ListErr::Retry) => (),
        };

        loop {
            let () = match self.try_delete::<false>(key, &mut del.try_del, tid, guard, pool) {
                Ok(()) => return Ok(()),
                Err(ListErr::Fail) => return Err(()),
                Err(ListErr::Retry) => (),
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{pmem::ralloc::Collectable, test_utils::tests::*};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 10_000;

    struct InsDelLook {
        inserts: [Insert<usize, usize>; COUNT],
        ins_lookups: [Lookup<usize, usize>; COUNT],
        deletes: [Delete<usize, usize>; COUNT],
        del_lookups: [Lookup<usize, usize>; COUNT],
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
            for i in 0..COUNT {
                Insert::filter(&mut m.inserts[i], tid, gc, pool);
                Lookup::filter(&mut m.ins_lookups[i], tid, gc, pool);
                Delete::filter(&mut m.deletes[i], tid, gc, pool);
                Lookup::filter(&mut m.del_lookups[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<InsDelLook> for TestRootObj<List<usize, usize>> {
        fn run(&self, ins_del_look: &mut InsDelLook, tid: usize, guard: &Guard, pool: &PoolHandle) {
            match tid {
                // T1: Check the execution results of other threads
                1 => {
                    // Check results
                    check_res(tid, NR_THREAD, COUNT);
                }
                // Threads other than T1 perform { insert; lookup; delete; lookup; }
                _ => {
                    #[cfg(feature = "simulate_tcrash")]
                    let rand = rdtscp() as usize % COUNT;

                    // enq; deq;
                    for i in 0..COUNT {
                        #[cfg(feature = "simulate_tcrash")]
                        if rand == i {
                            enable_killed(tid);
                        }

                        let key = compose(tid, i, i % tid);

                        // insert and lookup
                        // println!("insert k: {key}");
                        assert!(self
                            .obj
                            .insert::<true>(
                                key,
                                key,
                                &mut ins_del_look.inserts[i],
                                tid,
                                guard,
                                pool,
                            )
                            .is_ok());
                        let res = self.obj.lookup::<true>(
                            &key,
                            &mut ins_del_look.ins_lookups[i],
                            tid,
                            guard,
                            pool,
                        );
                        assert!(res.is_some());
                        // println!("lookup k: {key} -> value: {}", res.unwrap());

                        // Transfer the lookup result to the result array
                        let (tid, i, value) = decompose(*res.unwrap());
                        produce_res(tid, i, value);

                        // delete and lookup
                        // println!("delete k: {key}");
                        assert!(self
                            .obj
                            .delete::<true>(&key, &mut ins_del_look.deletes[i], tid, guard, pool)
                            .is_ok());
                        let res = self.obj.lookup::<true>(
                            &key,
                            &mut ins_del_look.del_lookups[i],
                            tid,
                            guard,
                            pool,
                        );
                        assert!(res.is_none());
                    }

                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    #[test]
    fn ins_del_look() {
        const FILE_NAME: &str = "list";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<List<usize, usize>>, InsDelLook>(
            FILE_NAME,
            FILE_SIZE,
            NR_THREAD + 1,
        );
    }
}
