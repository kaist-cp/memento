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
pub struct TryInsert<K, V> {
    tail: Checkpoint<PAtomic<Node<T>>>,
    insert: Cas,
}

#[derive(Debug)]
pub struct Insert<K, V> {
    node: Checkpoint<PAtomic<Node<K, V>>>,
}

#[derive(Debug)]
pub struct List<K, V> {
    head: CachePadded<DetectableCASAtomic<Node<K, V>>>,
}

impl<K, V> List<K, V> {
    // TODO: persistent
    fn find_harris<const REC: bool>(
        &self,
        key: K,
        value: V,
        find: &mut Find<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(bool, PShared<Node<K, V>>, PShared<Node<K, V>>), ()> {
        let mut prev_next = self.curr;
        let found = loop {
            let curr_node = some_or!(unsafe { self.curr.as_ref() }, break false);
            let next = curr_node.next.load(Ordering::Acquire, guard);

            if next.tag() != 0 {
                self.curr = next.with_tag(0);
                continue;
            }

            match curr_node.key.cmp(key) {
                Less => {
                    self.curr = next.with_tag(0);
                    self.prev = &curr_node.next;
                    prev_next = next;
                }
                Equal => break true,
                Greater => break false,
            }
        };

        // If prev and curr WERE adjacent, no need to clean up
        if prev_next == self.curr {
            return Ok(found);
        }

        // cleanup marked nodes between prev and curr
        self.prev
            .compare_exchange(
                prev_next,
                self.curr,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            )
            .map_err(|_| ())?;

        // defer_destroy from cursor.prev.load() to cursor.curr (exclusive)
        let mut node = prev_next;
        while node.with_tag(0) != self.curr {
            unsafe {
                let next = node.as_ref().unwrap().next.load(Ordering::Acquire, guard);
                guard.defer_destroy(node);
                node = next;
            }
        }

        Ok(found)
    }

    fn find<const REC: bool>(
        &self,
        key: K,
        find: &mut Find<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> (bool, PShared<Node<K, V>>, PShared<Node<K, V>>) {
        if let Ok(r, prev, curr) = find::<REC>(&mut cursor, key, guard) {
            return (r, prev, curr);
        }

        loop {
            if let Ok(r, prev, curr) = find::<false>(&mut cursor, key, guard) {
                return (r, prev, curr);
            }
        }
    }

    pub fn try_insert<const REC: bool>(
        &self,
        node: PShared<Node<K, V>>,
        prev: PShared<Node<K, V>>,
        next: PShared<Node<K, V>>,
        try_ins: &mut TryInsert<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        node.next.store(next, Ordering::Relaxed);
        persist_obj(unsafe { &node.deref(pool).next }, true);

        let prev_ref = unsafe { prev.deref(pool) };

        prev_ref
            .next
            .cas::<REC>(next, node, &mut try_ins.delete, tid, guard, pool)
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
        try_del: &mut TryDelete<K, V>,
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
            &mut try_ins.delete,
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
            &mut try_ins.delete,
            tid,
            guard,
            pool,
        ) {
            if e.tag() == 1 {
                return false;
            }
            next = e;
        }

        let prev_ref = unsafe { prev.deref() };
        if prev_ref
            .next
            .cas::<REC>(next, node, &mut try_ins.delete, tid, guard, pool)
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
