//! Persistent Location을 위한 모듈 곳곳에 쓰일 유틸리티 함수

/// `addr` 주소에서부터 T의 크기만큼 읽어서 T로 형변환
pub unsafe fn read_addr<'a, T>(addr: usize) -> &'a mut T {
    union U<T> {
        addr: usize,
        rf: *mut T,
    }
    &mut *U { addr }.rf
}
