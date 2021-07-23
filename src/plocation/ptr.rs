//! Persistent Pointer
use super::pool::*;
use super::utils::*;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// 풀에 속한 오브젝트를 가리킬 포인터
/// - 풀의 시작주소로부터의 offset을 가지고 있음
/// - 참조시 풀의 시작주소와 offset을 더한 주소를 참조  
#[derive(Default, Debug)]
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

    /// offset 반환
    pub fn get_off(&self) -> usize {
        self.offset
    }

    /// 절대주소 반환
    pub fn get_addr(&self) -> usize {
        Pool::start() + self.offset
    }
}

impl<T> From<usize> for PersistentPtr<T> {
    /// 풀의 offset 주소를 오브젝트로 간주하고 이를 참조하는 포인터 반환
    fn from(off: usize) -> Self {
        Self {
            offset: off,
            marker: PhantomData,
        }
    }
}

impl<T> Deref for PersistentPtr<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { read_addr(self.get_addr()) }
    }
}
impl<T> DerefMut for PersistentPtr<T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { read_addr(self.get_addr()) }
    }
}
