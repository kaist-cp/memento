//! Persistent Pointer
use crate::plocation::pool::*;
use crate::plocation::utils::*;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};

/// 풀에 속한 오브젝트를 가리킬 포인터
/// - 풀의 시작주소로부터의 offset을 가지고 있음
/// - 참조시 풀의 시작주소와 offset을 더한 주소를 참조  
#[derive(Default, Debug)]
pub struct PPtr<T> {
    offset: usize,
    marker: PhantomData<T>,
}

impl<T: Default> PPtr<T> {
    /// TODO: doc
    pub fn new() -> Self {
        // T의 크기만큼 할당 후 포인터 얻음
        let mut slf = Self {
            offset: Pool::alloc(mem::size_of::<T>()),
            marker: PhantomData,
        };
        // T 내부 초기화
        *slf = T::default();
        slf
    }

    /// TODO: doc
    pub fn from_off(off: usize) -> Self {
        Self {
            offset: off,
            marker: PhantomData,
        }
    }

    /// null 포인터 반환
    pub fn null() -> Self {
        // TODO: 현재는 usize::MAX를 null 식별자로 사용중. 더 좋은 방법 찾기?
        Self {
            offset: usize::MAX,
            marker: PhantomData,
        }
    }

    /// TODO: doc
    pub fn is_null(&self) -> bool {
        self.offset == usize::MAX
    }

    /// TODO: doc
    pub fn get_off(&self) -> usize {
        self.offset
    }

    /// TODO: doc
    pub fn get_addr(&self) -> usize {
        unsafe { POOL_START + self.offset }
    }
}

impl<T: Default> Deref for PPtr<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { read_addr(self.get_addr()) }
    }
}
impl<T: Default> DerefMut for PPtr<T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { read_addr(self.get_addr()) }
    }
}
