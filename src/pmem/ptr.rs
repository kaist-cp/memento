//! Persistent Pointer
use super::pool::PoolHandle;
use std::marker::PhantomData;

/// 상대주소의 NULL 식별자
const NULL_OFFSET: usize = 0;

/// 풀에 속한 오브젝트를 가리킬 포인터
/// - 풀의 시작주소로부터의 offset을 가지고 있음
/// - 참조시 풀의 시작주소와 offset을 더한 절대주소를 참조
// `T: ?Sized`인 이유: `PPtr::null()`을 사용해야하는 Atomic 포인터의 T가 ?Sized임
// NOTE: plocation offset의 align이 안맞을 수 있음. 주의 필요
#[derive(Debug)]
pub struct PPtr<T: ?Sized> {
    offset: usize,
    _marker: PhantomData<*const T>,
}

impl<T: ?Sized> Clone for PPtr<T> {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized> Copy for PPtr<T> {}

impl<T: ?Sized> PPtr<T> {
    /// null 포인터 반환
    pub fn null() -> Self {
        Self {
            offset: NULL_OFFSET,
            _marker: PhantomData,
        }
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

    /// null 포인터인지 확인
    pub fn is_null(self) -> bool {
        self.offset == NULL_OFFSET
    }
}

impl<T> PPtr<T> {
    /// 절대주소 참조
    ///
    /// # Safety
    ///
    /// TODO: 동시에 풀 여러개를 열 수있다면 pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn deref(self, pool: &PoolHandle) -> &'_ T {
        &*((pool.start() + self.offset) as *const T)
    }

    /// 절대주소 mutable 참조
    ///
    /// # Safety
    ///
    /// TODO: 동시에 풀 여러개를 열 수있다면 pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn deref_mut(self, pool: &PoolHandle) -> &'_ mut T {
        &mut *((pool.start() + self.offset) as *mut T)
    }
}

/// reference를 persistent ptr로 바꿔줌
pub trait AsPPtr {
    /// reference를 persistent ptr로 바꿔줌
    ///
    /// # Safety
    ///
    /// object가 `pool`에 속한 reference여야 함
    unsafe fn as_pptr(&self, pool: &PoolHandle) -> PPtr<Self>;
}

impl<T> AsPPtr for T {
    unsafe fn as_pptr(&self, pool: &PoolHandle) -> PPtr<Self> {
        PPtr {
            offset: self as *const T as usize - pool.start(),
            _marker: PhantomData,
        }
    }
}

impl<T> From<usize> for PPtr<T> {
    /// 주어진 offset을 T obj의 시작 주소로 간주하고 이를 참조하는 포인터 반환
    fn from(off: usize) -> Self {
        Self {
            offset: off,
            _marker: PhantomData,
        }
    }
}

impl<T> PartialEq<PPtr<T>> for PPtr<T> {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl<T> Eq for PPtr<T> {}
