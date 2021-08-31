//! Persistent Allocator

use std::alloc::Layout;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Persistent Allocator
// TODO: global obj로 취급하지 않기
// TODO: 현재는 adhoc하게 alloc만 가능하게 돼있음. 디테일한 구현 필요
#[derive(Debug, Default)]
pub struct Allocator {
    /// 할당 가능한 다음위치
    next: AtomicUsize,
}

impl Allocator {
    /// Layout에 맞게 메모리 할당 후 주소 반환
    // TODO: PersistentOp
    pub fn alloc(&self, layout: Layout) -> usize {
        let (size, align) = (layout.size(), layout.align());
        let size_aligned = (size + align - 1) & !(align - 1);
        self.next.fetch_add(size_aligned, Ordering::SeqCst)
    }

    /// free
    // TODO: PersistentOp
    pub fn free() {
        todo!()
    }
}
