//! Persistent Allocator

// 참고
// - https://doc.rust-lang.org/std/alloc/trait.Allocator.html
// - https://os.phil-opp.com/allocator-designs/

use std::alloc::Layout;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Persistent Allocator
#[derive(Debug)]
pub struct Allocator {
    /// allocator 입장에서 할당 가능한 다음위치
    // TODO: 현재는 incremental하게 alloc만 가능케 구현돼있음. 디테일한 구현 필요
    next: AtomicUsize,
}

impl Allocator {
    /// new
    pub fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
        }
    }

    /// Layout에 맞게 메모리 할당 후 주소 반환
    pub fn alloc(&self, layout: Layout) -> usize {
        let (size, align) = (layout.size(), layout.align());
        let size_aligned = (size + align - 1) & !(align - 1);
        self.next.fetch_add(size_aligned, Ordering::SeqCst)
    }

    /// free
    pub fn free() {
        todo!()
    }
}
