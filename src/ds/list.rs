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
pub struct Node<K, V> {
    key: K,
    value: V,
    next: DetectableCASAtomic<Self>,
}

impl<T: Collectable> From<(K, V)> for Node<K, V> {
    fn from((key, value): (K, V)) -> Self {
        Self {
            key,
            value,
            next: DetectableCASAtomic::default(),
        }
    }
}

#[derive(Debug)]
pub struct Harris<K, V> {
    result: Checkpoint<(
        bool,
        PPtr<DetectableCASAtomic<K, V>>,
        PAtomic<Node<K, V>>,
        PAtomic<Node<K, V>>,
    )>,
    help: Cas,
}

#[derive(Debug)]
pub struct Find<K, V> {
    harris: Harris<K, V>,
}

#[derive(Debug)]
pub struct Lookup<K, V> {
    find: Find<K, V>,
}

#[derive(Debug)]
pub struct TryInsert {
    insert: Cas,
}

#[derive(Debug)]
pub struct Insert<K, V> {
    node: Checkpoint<PAtomic<Node<K, V>>>,
    find: Find<K, V>,
    try_ins: TryInsert,
}

#[derive(Debug)]
pub struct TryDelete {
    logical: Cas,
    physical: Cas,
}

#[derive(Debug)]
pub struct Delete<K, V> {
    find: Find<K, V>,
    try_del: TryDelete,
}

#[derive(Debug)]
pub struct List<K, V> {
    head: CachePadded<DetectableCASAtomic<Node<K, V>>>,
}

impl<K, V> List<K, V> {
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
