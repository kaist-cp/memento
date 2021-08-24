//! Persistent Pointer
use super::pool::PoolHandle;
use std::marker::PhantomData;

/// 풀에 속한 오브젝트를 가리킬 포인터
/// - 풀의 시작주소로부터의 offset을 가지고 있음
/// - 참조시 풀의 시작주소와 offset을 더한 절대주소를 참조  
#[derive(Debug)]
pub struct PersistentPtr<T> {
    offset: usize,
    marker: PhantomData<T>,
}

impl<T> PersistentPtr<T> {
    /// null 포인터 반환
    pub fn null() -> Self {
        // TODO: 현재는 usize::MAX를 null 식별자로 사용중. 더 좋은 방법 찾기?
        Self {
            offset: usize::MAX,
            marker: PhantomData,
        }
    }

    /// null 포인터인지 확인
    pub fn is_null(&self) -> bool {
        self.offset == usize::MAX
    }

    /// 절대주소 참조
    ///
    /// # Safety
    ///
    /// TODO: 동시에 풀 여러개를 열 수있다면 pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn deref(&self, pool: &PoolHandle) -> &T {
        &*((pool.start() + self.offset) as *const T)
    }

    /// 절대주소 mutable 참조
    ///
    /// # Safety
    ///
    /// TODO: 동시에 풀 여러개를 열 수있다면 pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn deref_mut(&mut self, pool: &PoolHandle) -> &mut T {
        &mut *((pool.start() + self.offset) as *mut T)
    }

    /// offset으로 변환
    ///
    /// # Example
    ///
    /// pool에 할당하면 나오는 PersistentPtr를 Atomic Pointer로 변환하기 위해 필요
    /// - `Owned::from_usize(ptr.into_offset())`
    pub fn into_offset(self) -> usize {
        self.offset
    }
}

impl<T> From<usize> for PersistentPtr<T> {
    /// 주어진 offset을 T obj의 시작 주소로 간주하고 이를 참조하는 포인터 반환
    fn from(off: usize) -> Self {
        Self {
            offset: off,
            marker: PhantomData,
        }
    }
}
