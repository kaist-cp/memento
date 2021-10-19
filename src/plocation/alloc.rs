//! Persistent Allocator

use std::alloc::Layout;
use std::sync::atomic::{AtomicUsize, Ordering};

/// size를 align에 맞춤
#[inline]
pub fn align_up(size: usize, align: usize) -> usize {
    (size + align - 1) & !(align - 1)
}

/// Persistent Allocator
// TODO: global obj로 취급하지 않기
// TODO: std `GlobalAlloc` trait과 유사하게 API 구현? (https://doc.rust-lang.org/std/alloc/trait.GlobalAlloc.html)
#[derive(Debug, Default)]
pub struct Allocator {
    /// 할당 가능한 다음위치 (풀 시작주소로부터의 상대주소)
    next: AtomicUsize,
    // TODO: allocator가 끝 주소도 알게하고 "더이상 할당할 공간 없음"을 식별 가능하게 하기
}

impl Allocator {
    /// TODO
    pub fn new(init: usize) -> Self {
        Self {
            next: AtomicUsize::new(init),
        }
    }

    /// Layout에 맞게 메모리 할당 후 주소 반환
    // TODO: PersistentOp
    pub fn alloc(&self, layout: Layout) -> usize {
        let (size, align) = (layout.size(), layout.align());
        let size_aligned = align_up(size, align);

        loop {
            let cur = self.next.load(Ordering::SeqCst);

            // 할당되는 주소는 align의 배수여야함
            // e.g. 현재 next가 20이여도, 할당할 객체의 align이 16이라면 32(base_aligned)에 할당. 이 로직에서 20~32는 버려짐
            let base_aligned = align_up(cur, align);
            // e.g. align된 객체 A의 크기가 16이라면, A는 32~48을 사용. 따라서 다음 next는 48
            let next = base_aligned + size_aligned;
            if self
                .next
                .compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return base_aligned;
            }
        }
    }

    /// free
    // TODO: PersistentOp
    pub fn free() {
        todo!()
    }
}
