//! Persistent Location을 위한 모듈 곳곳에 쓰일 유틸리티 함수

/// `addr` 주소에서부터 T의 크기만큼 읽어서 T로 형변환
///
/// # Safety
/// TODO: Safety doc 작성?
pub unsafe fn read_addr<'a, T>(addr: usize) -> &'a mut T {
    &mut *(addr as *mut T)
}
