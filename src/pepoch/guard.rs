//! Persistent Guard
//!
//! TODO: Persistent SMR 필요하다 판단되면 crossbeam의 guard 가져와서 persistent version으로 수정

use std::marker::PhantomData;

/// Persistent Guard
#[derive(Debug)]
pub struct Guard<'a> {
    /// PoolHandle의 lifetime보다 짧게하기 위한 marker
    pub _marker: PhantomData<&'a ()>,
}
