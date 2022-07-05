//! Persistent Harris List

use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, DetectableCASAtomic};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use etrace::ok_or;
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
use crate::*;

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
pub struct Harris<K, V: Collectable> {
    result: Checkpoint<(
        bool,
        PPtr<DetectableCASAtomic<K, V>>,
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
pub struct Find<K, V: Collectable> {
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
        Lookup::filter(&mut lookup.find, tid, gc, pool);
    }
}

#[derive(Debug)]
pub struct TryInsert {
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
pub struct TryDelete {
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

#[derive(Debug)]
pub struct List<K, V: Collectable> {
    head: CachePadded<DetectableCASAtomic<Node<K, V>>>,
}

impl<K, V: Collectable> PDefault for List<K, V> {
    fn pdefault(pool: &PoolHandle) -> Self {
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

impl<K, V: Collectable> List<K, V> {
    fn harris<const REC: bool>(
        &self,
        key: K,
        harris: &mut Harris<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(bool, &DetectableCASAtomic<Node<K, V>>, PShared<Node<K, V>>), ()> {
        let mut prev = &self.head;
        let mut curr = self.head.load(Ordering::SeqCst, guard, pool);
        let mut prev_next = curr;

        let found = loop {
            let curr_node = some_or!(unsafe { curr.as_ref() }, break false);
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

        let (found, prev, curr, prev_next) = ok_or!(
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
            (
                e.0,
                unsafe { e.1.deref(pool) },
                e.2.load(Ordering::Relaxed, guard),
                e.3.load(Ordering::Relaxed, guard)
            )
        );
        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == curr {
            return Ok(found, prev, curr);
        }

        // cleanup marked nodes between prev and curr
        prev.cas::<REC>(prev_next, curr, &mut harris.help, tid, guard, pool)
            .map_err(|_| ())?;

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while node.with_tag(0) != curr {
            unsafe {
                let next = node
                    .as_ref()
                    .unwrap()
                    .next
                    .load(Ordering::Acquire, guard, pool);
                guard.defer_pdestroy(node);
                node = next;
            }
        }

        Ok(found, prev, curr)
    }

    fn find<const REC: bool>(
        &self,
        key: K,
        find: &mut Find<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> (bool, &DetectableCASAtomic<Node<K, V>>, PShared<Node<K, V>>) {
        if let Ok(r, prev, curr) = self.harris::<REC>(key, &mut find.harris, tid, guard, pool) {
            return (r, prev, curr);
        }

        loop {
            if let Ok(r, prev, curr) = self.harris::<false>(key, &mut find.harris, tid, guard, pool)
            {
                return (r, prev, curr);
            }
        }
    }

    pub fn lookup<const REC: bool>(
        &self,
        key: K,
        look: &mut Lookup<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Option<&V> {
        let (found, _, curr) = self.find::<REC>(key, &mut look.find, tid, guard, pool);
        if found {
            unsafe { curr.as_ref().map(|n| &n.value) }
        } else {
            None
        }
    }

    pub fn try_insert<const REC: bool>(
        &self,
        node: PShared<Node<K, V>>,
        prev: &DetectableCASAtomic<Node<K, V>>,
        next: PShared<Node<K, V>>,
        try_ins: &mut TryInsert<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        node.next.store(next, Ordering::Relaxed);
        persist_obj(unsafe { &node.deref(pool).next }, true);

        prev.cas::<REC>(next, node, &mut try_ins.insert, tid, guard, pool)
            .map(|_| ())
            .map_err(|_| ())
    }

    pub fn insert<const REC: bool>(
        &self,
        key: K,
        value: V,
        ins: &mut Insert<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let node = POwned::new(Node::from((key, value)));
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

        let (found, prev, curr) = self.find::<REC>(&node.key, &mut ins.find, tid, guard, pool);
        if found {
            unsafe { guard.defer_pdestroy(node) };
            return false;
        }

        if self
            .try_insert::<REC>(node, prev, curr, ins.try_ins, tid, guard, pool)
            .is_ok()
        {
            return Ok(());
        }

        loop {
            let (found, prev, curr) =
                self.find::<false>(&node.key, &mut ins.find, tid, guard, pool);
            if found {
                unsafe { guard.defer_pdestroy(node) };
                return false;
            }

            if self
                .try_insert::<false>(node, prev, curr, ins.try_ins, tid, guard, pool)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    pub fn try_delete<const REC: bool>(
        &self,
        prev: PShared<Node<K, V>>,
        curr: PShared<Node<K, V>>,
        try_del: &mut TryDelete,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        let curr_ref = unsafe { curr.deref() };

        // FAO-like..
        let mut next = curr_ref.next.load(Ordering::SeqCst, guard, pool);
        if next.tag() == 1 {
            return false;
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
                return false;
            }
            next = e;
        }

        while let Err(e) = curr_ref.next.cas::<false>(
            next,
            next.with_tag(1),
            &mut try_del.logical,
            tid,
            guard,
            pool,
        ) {
            if e.tag() == 1 {
                return false;
            }
            next = e;
        }

        if prev
            .cas::<REC>(next, node, &mut try_del.physical, tid, guard, pool)
            .is_ok()
        {
            unsafe { guard.defer_pdestroy(curr) };
        }

        true
    }

    pub fn delete<const REC: bool>(
        &self,
        key: K,
        del: &mut Delete<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        let (found, prev, curr) = self.find::<REC>(&node.key, &mut del.find, tid, guard, pool);
        if !found {
            return false;
        }

        if self
            .try_delete::<REC>(node, prev, curr, del.try_del, tid, guard, pool)
            .is_ok()
        {
            return true;
        }

        loop {
            let (found, prev, curr) =
                self.find::<false>(&node.key, &mut del.find, tid, guard, pool);
            if !found {
                return false;
            }

            if self
                .try_delete::<false>(node, prev, curr, del.try_del, tid, guard, pool)
                .is_ok()
            {
                return true;
            }
        }
    }
}
