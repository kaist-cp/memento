//! Persistent list

use std::fmt::Debug;

use crossbeam_epoch::{Atomic, Shared};

use crate::persistent::POp;

#[allow(dead_code)] // TODO: 지우기
struct Node<K, V> {
    key: K,
    value: V,
    next: Atomic<Node<K, V>>,
}

/// TODO: doc
#[derive(Debug)]
#[allow(dead_code)] // TODO: 지우기
pub struct Insert<K, V> {
    // TODO: 구현
    node: Atomic<Node<K, V>>,
}

impl<K, V> Default for Insert<K, V> {
    fn default() -> Self {
        Self {
            node: Atomic::null(),
        }
    }
}

impl<'l, K, V> POp<'l, &'l List<K, V>> for Insert<K, V> {
    type Input = (K, V);
    type Output = bool;

    fn run(&mut self, object: &List<K, V>, input: Self::Input) -> Self::Output {
        let _ = (object, input);
        unimplemented!()
    }

    fn reset(&mut self, nested: bool) {
        let _ = nested;
        unimplemented!()
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct Remove {
    // TODO: 구현
}

impl<'l, K, V> POp<'l, &'l List<K, V>> for Remove {
    type Input = usize;
    type Output = Result<(), ()>; // TODO: return data

    fn run(&mut self, object: &List<K, V>, input: Self::Input) -> Self::Output {
        let _ = (object, input);
        unimplemented!()
    }

    fn reset(&mut self, nested: bool) {
        let _ = nested;
        unimplemented!()
    }
}

/// TODO: doc
#[derive(Debug)]
#[allow(dead_code)] // TODO: 지우기
pub struct Cursor<'n, K, V> {
    prev: &'n Atomic<Node<K, V>>,
    curr: Shared<'n, Node<K, V>>,
}

/// TODO: doc
#[derive(Debug)]
#[allow(dead_code)] // TODO: 지우기
pub struct List<K, V> {
    head: Atomic<Node<K, V>>,
}

impl<K, V> Default for List<K, V> {
    fn default() -> Self {
        Self {
            head: Atomic::null(),
        }
    }
}

impl<K, V> List<K, V> {
    /// TODO: doc
    pub fn head(&self) -> Cursor<'_, K, V> {
        unimplemented!()
    }
}

impl<K, V> Iterator for Cursor<'_, K, V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}
