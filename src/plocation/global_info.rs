//! 풀이 열려있는 동안 global하게 유지할 정보들 (e.g. 풀의 시작주소, 풀의 메타데이터)
//!
//! 데이터 접근성능을 위해 DRAM 혹은 PM에서 유지할 정보를 구분
//! - DRAM에서 유지: `POOL_RUNTIME_INFO`
//! - PM에서 유지: `POOL_METADATA`

use memmap::*;

/// 풀의 런타임 정보
/// - 이 오브젝트에 접근하는 것은 DRAM 접근을 의미
/// - 풀 열때/닫을 때: 열 때는 Some으로 만들며 풀의 시작주소 등을 세팅, 닫을 때는 None으로 만듦
/// - Persistent Pointer가 참조할 때: 이 정보에 담긴 풀의 시작주소를 base로 사용
pub static mut POOL_RUNTIME_INFO: Option<PoolRuntimeInfo> = None;

// TODO: 풀의 메타데이터
// - 이 오브젝트에 접근하는 것은 PM 접근을 의미
// - 현재는 필요없음: 간단한 버전이기 때문에 풀이 열려있는 동안 풀의 메타데이터 계속 접근할 필요없음
// - 향후엔 필요할듯함: e.g. allocator를 위한 메타데이터를 계속 업데이트 해줘야할 수도
// pub static mut POOL_METADATA: Pool = Pool { ... };

/// 풀의 런타임 정보를 담는 역할
#[derive(Debug)]
pub struct PoolRuntimeInfo {
    /// 메모리 매핑에 사용한 오브젝트 (drop으로 인해 매핑 해제되지 않게끔 들고 있어야함)
    mmap: MmapMut,

    /// 풀의 시작 주소
    start: usize,

    /// 풀의 길이
    len: usize,
}

impl PoolRuntimeInfo {
    /// 풀의 런타임 정보 세팅
    pub fn init(mmap: MmapMut, start: usize, len: usize) {
        unsafe {
            POOL_RUNTIME_INFO = Some(PoolRuntimeInfo { mmap, start, len });
        }
    }

    /// 풀의 런타임 정보 삭제
    pub fn clear() {
        unsafe {
            // 메모리 매핑에 사용한 오브젝트가 `mmap` 필드에 저장되어있었다면 이때 매핑 해제됨
            POOL_RUNTIME_INFO = None;
        }
    }

    /// 풀의 런타임 정보 세팅되어있는지 확인
    pub fn is_initialized() -> bool {
        unsafe { POOL_RUNTIME_INFO.is_some() }
    }

    /// 풀의 런타임 정보 중 시작주소 반환
    ///
    /// # Safety
    /// `POOL_RUNTIME_INFO`가 Some인지는 호출자가 확인해야함
    pub unsafe fn start() -> usize {
        POOL_RUNTIME_INFO.as_ref().unwrap().start
    }

    /// 풀의 런타임 정보 중 풀의 길이 반환
    ///
    /// # Safety
    /// `POOL_RUNTIME_INFO`가 Some인지는 호출자가 확인해야함
    pub unsafe fn len() -> usize {
        POOL_RUNTIME_INFO.as_ref().unwrap().len
    }
}
