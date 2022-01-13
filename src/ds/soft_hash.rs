//! TODO doc
use std::{borrow::BorrowMut, ptr::null_mut};

use crate::pmem::{ssmem_alloc, ssmem_allocator, AsPPtr};

use super::soft_list::SOFTList;

const BUCKET_NUM: usize = 16777216;

struct SOFTHashTable<T> {
    table: [SOFTList<T>; BUCKET_NUM],
}

impl<T> SOFTHashTable<T> {
    fn insert(&self, k: usize, item: T) {
        todo!()
    }

    fn remove(&self, k: usize) {
        todo!()
    }

    fn contains(&self, k: usize) {
        todo!()
    }

    fn getBucket(&self, k: usize) -> &'static SOFTList<T> {
        todo!()
    }

    fn SOFTrecovery() {
        todo!()
    }
}
