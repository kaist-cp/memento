//! Persistent Harris List

use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, DetectableCASAtomic};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use etrace::{ok_or, some_or};
use std::cmp::Ordering::{Equal, Greater, Less};

use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
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
    help: Cas,
}

impl<K, V: Collectable> Default for Harris<K, V> {
    fn default() -> Self {
        Self {
            result: Default::default(),
            help: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Harris<K, V> {
    fn filter(harris: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut harris.result, tid, gc, pool);
        Cas::filter(&mut harris.help, tid, gc, pool);
    }
}

#[derive(Debug)]
struct Find<K, V: Collectable> {
    harris: Harris<K, V>,
}

impl<K, V: Collectable> Default for Find<K, V> {
    fn default() -> Self {
        Self {
            harris: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Find<K, V> {
    fn filter(find: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Harris::filter(&mut find.harris, tid, gc, pool);
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
struct TryInsert {
    insert: Cas,
}

impl Default for TryInsert {
    fn default() -> Self {
        Self {
            insert: Default::default(),
        }
    }
}

impl Collectable for TryInsert {
    fn filter(try_ins: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Cas::filter(&mut try_ins.insert, tid, gc, pool);
    }
}

/// Insert memento
#[derive(Debug)]
pub struct Insert<K, V: Collectable> {
    node: Checkpoint<PAtomic<Node<K, V>>>,
    find: Find<K, V>,
    try_ins: TryInsert,
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
struct TryDelete {
    logical: Cas,
    physical: Cas,
}

impl Default for TryDelete {
    fn default() -> Self {
        Self {
            logical: Default::default(),
            physical: Default::default(),
        }
    }
}

impl Collectable for TryDelete {
    fn filter(try_del: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Cas::filter(&mut try_del.logical, tid, gc, pool);
        Cas::filter(&mut try_del.physical, tid, gc, pool);
    }
}

/// Delete memento
#[derive(Debug)]
pub struct Delete<K, V: Collectable> {
    find: Find<K, V>,
    try_del: TryDelete,
}

impl<K, V: Collectable> Default for Delete<K, V> {
    fn default() -> Self {
        Self {
            find: Default::default(),
            try_del: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Delete<K, V> {
    fn filter(del: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Find::filter(&mut del.find, tid, gc, pool);
        TryDelete::filter(&mut del.try_del, tid, gc, pool);
    }
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
    fn harris<'g, const REC: bool>(
        &self,
        key: &K,
        harris: &mut Harris<K, V>,
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

        let chk = ok_or!(
            harris.result.checkpoint::<REC>(
                (
                    found,
                    unsafe { prev.as_pptr(pool) },
                    PAtomic::from(curr),
                    PAtomic::from(prev_next)
                ),
                tid,
                pool
            ),
            e,
            e.current
        );
        let (found, prev, curr, prev_next) = (
            chk.0,
            unsafe { chk.1.deref(pool) },
            chk.2.load(Ordering::Relaxed, guard),
            chk.3.load(Ordering::Relaxed, guard),
        );

        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == curr {
            return Ok((found, prev, curr));
        }

        // cleanup marked nodes between prev and curr
        prev.cas::<REC>(prev_next, curr, &mut harris.help, tid, guard, pool)
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

        Ok((found, prev, curr))
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
        if let Ok(res) = self.harris::<REC>(key, &mut find.harris, tid, guard, pool) {
            return res;
        }

        loop {
            if let Ok(res) = self.harris::<false>(key, &mut find.harris, tid, guard, pool) {
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
        prev: &DetectableCASAtomic<Node<K, V>>,
        next: PShared<'_, Node<K, V>>,
        try_ins: &mut TryInsert,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let node_ref = unsafe { node.deref(pool) };
        node_ref.next.inner.store(next, Ordering::Relaxed);
        persist_obj(unsafe { &node.deref(pool).next }, true);

        prev.cas::<REC>(next, node, &mut try_ins.insert, tid, guard, pool)
            .map(|_| ())
            .map_err(|_| ())
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
        let node = POwned::new(Node::from((key, value)), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = ok_or!(
            ins.node.checkpoint::<REC>(PAtomic::from(node), tid, pool),
            e,
            unsafe {
                drop(
                    e.new
                        .load(Ordering::Relaxed, epoch::unprotected())
                        .into_owned(),
                );
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);
        let node_ref = unsafe { node.deref(pool) };

        let (found, prev, curr) = self.find::<REC>(&node_ref.key, &mut ins.find, tid, guard, pool);
        if found {
            unsafe { guard.defer_pdestroy(node) };
            return Err(());
        }

        if self
            .try_insert::<REC>(node, prev, curr, &mut ins.try_ins, tid, guard, pool)
            .is_ok()
        {
            return Ok(());
        }

        loop {
            let (found, prev, curr) =
                self.find::<false>(&node_ref.key, &mut ins.find, tid, guard, pool);
            if found {
                unsafe { guard.defer_pdestroy(node) };
                return Err(());
            }

            if self
                .try_insert::<false>(node, prev, curr, &mut ins.try_ins, tid, guard, pool)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn try_delete<const REC: bool>(
        &self,
        prev: &DetectableCASAtomic<Node<K, V>>,
        curr: PShared<'_, Node<K, V>>,
        try_del: &mut TryDelete,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let curr_ref = unsafe { curr.deref(pool) };

        // FAO-like..
        let mut next = curr_ref.next.load(Ordering::SeqCst, guard, pool);
        if next.tag() == 1 {
            return Err(());
        }
        if let Err(e) = curr_ref.next.cas::<REC>(
            next,
            next.with_tag(1),
            &mut try_del.logical,
            tid,
            guard,
            pool,
        ) {
            if e.tag() == 1 {
                return Err(());
            }
            next = e;

            while let Err(e) = curr_ref.next.cas::<false>(
                next,
                next.with_tag(1),
                &mut try_del.logical,
                tid,
                guard,
                pool,
            ) {
                if e.tag() == 1 {
                    return Err(());
                }
                next = e;
            }
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
        let (found, prev, curr) = self.find::<REC>(key, &mut del.find, tid, guard, pool);
        if !found {
            return Err(());
        }

        if self
            .try_delete::<REC>(prev, curr, &mut del.try_del, tid, guard, pool)
            .is_ok()
        {
            return Ok(());
        }

        loop {
            let (found, prev, curr) = self.find::<false>(key, &mut del.find, tid, guard, pool);
            if !found {
                return Err(());
            }

            if self
                .try_delete::<false>(prev, curr, &mut del.try_del, tid, guard, pool)
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{pmem::ralloc::Collectable, test_utils::tests::*};

    const NR_THREAD: usize = 1;
    const COUNT: usize = 1;

    struct InsDelLook {
        inserts: [Insert<usize, usize>; COUNT],
        deletes: [Delete<usize, usize>; COUNT],
        lookups: [Lookup<usize, usize>; COUNT],
    }

    impl Default for InsDelLook {
        fn default() -> Self {
            Self {
                inserts: array_init::array_init(|_| Default::default()),
                deletes: array_init::array_init(|_| Default::default()),
                lookups: array_init::array_init(|_| Default::default()),
            }
        }
    }

    impl Collectable for InsDelLook {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..COUNT {
                Insert::filter(&mut m.inserts[i], tid, gc, pool);
                Delete::filter(&mut m.deletes[i], tid, gc, pool);
                Lookup::filter(&mut m.lookups[i], tid, gc, pool);
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
                    // enq; deq;
                    for i in 0..COUNT {
                        let key = compose(tid, i, i % tid);

                        // insert and lookup
                        println!("insert k: {key}");
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
                            &mut ins_del_look.lookups[i],
                            tid,
                            guard,
                            pool,
                        );
                        assert!(res.is_some());
                        println!("lookup k: {key} -> value: {}", res.unwrap());

                        // Transfer the lookup result to the result array
                        let (tid, i, value) = decompose(*res.unwrap());
                        produce_res(tid, i, value);

                        // delete and lookup
                        println!("delete k: {key}");
                        assert!(self
                            .obj
                            .delete::<true>(&key, &mut ins_del_look.deletes[i], tid, guard, pool)
                            .is_ok());
                        let res = self.obj.lookup::<true>(
                            &key,
                            &mut ins_del_look.lookups[i],
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
