//! 풀이 열려있는 동안 풀의 정보(e.g. 풀의 시작주소, 풀의 메타데이터)를 들고 있을 global object

use super::pool::PoolHandle;

static mut GLOBAL_POOL: Option<PoolHandle> = None;

/// 글로벌 풀 세팅
pub fn init(pool: PoolHandle) {
    unsafe {
        GLOBAL_POOL = Some(pool);
    }
}

/// 글로벌 풀 clear
pub fn clear() {
    unsafe {
        GLOBAL_POOL = None;
    }
}

/// 글로벌 풀 읽기
pub fn global_pool() -> Option<&'static PoolHandle> {
    unsafe { GLOBAL_POOL.as_ref() }
}