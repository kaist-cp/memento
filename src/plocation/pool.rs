//! Persistent Memory Pool
//!
//! 파일을 persistent heap으로서 가상주소에 매핑하고, 그 메모리 영역을 관리하는 메모리 "풀"

use std::fs::OpenOptions;
use std::io::{Error, ErrorKind};
use std::mem;

use crate::persistent::*;

use super::global::{self, global_pool};
use super::ptr::PersistentPtr;
use memmap::*;

/// 열린 풀을 관리하기 위한 풀 핸들러
///
/// # 핸들러 사용예시
///
/// ```
/// // TODO: client 인터페이스에 맞게 doc test 수정
/// //
/// // use compositional_persistent_object::plocation::pool::Pool;
/// //
/// // // 풀 생성 후 열어서 풀 핸들러 얻기
/// // let _ = Pool::create::<i32>("foo.pool", 8 * 1024);
/// // let pool_handle = Pool::open("foo.pool").unwrap();
/// //
/// // // 핸들러로부터 루트 오브젝트를 가져와서 사용
/// // let mut head = pool_handle.get_root::<i32>().unwrap();
/// // let mut head = unsafe { head.deref_mut() };
/// // *head = 5;
/// // assert_eq!(*head, 5);
/// ```
#[derive(Debug)]
pub struct PoolHandle {
    /// 메모리 매핑에 사용한 오브젝트 (drop으로 인해 매핑 해제되지 않게끔 들고 있어야함)
    mmap: MmapMut,

    /// 풀의 길이
    len: usize,
}

impl PoolHandle {
    /// 풀의 시작주소 반환
    #[inline]
    pub fn start(&self) -> usize {
        self.mmap.as_ptr() as usize
    }

    /// 풀의 끝주소 반환
    #[inline]
    pub fn end(&self) -> usize {
        self.start() + self.len
    }

    /// 풀의 루트(루트 오브젝트/루트 클라이언트 tuple)를 가리키는 포인터 반환
    #[inline]
    pub fn get_root<O: PersistentOp<C>, C: PersistentClient>(
        &self,
    ) -> Result<PersistentPtr<(O, C)>, Error> {
        // TODO: 잘못된 타입으로 가져오려하면 에러 반환
        Ok(PersistentPtr::from(self.pool().root_offset))
    }

    /// 풀에 T의 크기만큼 할당 후 이를 가리키는 포인터 얻음
    #[inline]
    pub fn alloc<T>(&self) -> PersistentPtr<T> {
        self.pool().alloc::<T>()
    }

    /// persistent pointer가 가리키는 풀 내부의 메모리 블록 할당해제
    #[inline]
    pub fn free<T>(&self, pptr: &mut PersistentPtr<T>) {
        self.pool().free(pptr)
    }

    #[inline]
    fn pool(&self) -> &Pool {
        unsafe { &*(self.start() as *const Pool) }
    }
}

/// 풀 열기/닫기 및 메타데이터를 관리하는 역할
///
/// # Pool Address Layout
///
/// [ metadata | (root obj, root client) |            ...               ]
/// ^ base     ^ base + root offset                         ^ end
#[derive(Debug)]
pub struct Pool {
    /// 풀의 시작주소로부터 루트 오브젝트/클라이언트까지의 거리
    root_offset: usize,

    /// 풀이 잘 초기화 되었는지 여부
    // TODO: 파일 생성시 초기값=false 보장가능한지 알아보기
    is_initialized: bool,
    // TODO: 풀의 메타데이터는 여기에 필드로 추가
}

impl Pool {
    /// 풀 내부 초기화 (메타데이터, 루트 오브젝트/클라이언트 초기화)
    fn init<O: Default + PersistentOp<C>, C: PersistentClient>(&mut self, start: usize) {
        // e.g. 메타데이터 크기(size_of::<Pool>)가 16이라면, 루트는 풀의 시작주소+16에 위치
        self.root_offset = mem::size_of::<Pool>();

        // 루트 오브젝트/클라이언트 초기화
        let (root_obj, root_client) = unsafe { &mut *((start + self.root_offset) as *mut (O, C)) };
        *root_obj = O::default();
        *root_client = C::default();

        // "초기화 완료" 표시
        self.is_initialized = true;
    }

    /// 풀 생성 (풀로서 사용할 파일을 생성하고 풀 레이아웃에 맞게 파일의 내부구조 초기화)
    pub fn create<O: Default + PersistentOp<C>, C: PersistentClient>(
        filepath: &str,
        size: usize,
    ) -> Result<(), Error> {
        // 1. 파일 생성 및 크기 세팅 (파일이 이미 존재하면 실패)
        if let Some(prefix) = std::path::Path::new(filepath).parent() {
            // e.g. "a/b/c.txt"라면, a/b/ 폴더도 만들어줌
            std::fs::create_dir_all(prefix).unwrap();
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(filepath)?;
        file.set_len(size as u64)?;

        // 2. 파일을 풀 레이아웃에 맞게 초기화
        let mmap = unsafe { memmap::MmapOptions::new().map_mut(&file)? };
        let pool = unsafe { &mut *(mmap.as_ptr() as *mut Pool) };
        pool.init::<O, C>(mmap.as_ptr() as usize);
        Ok(())
    }

    /// 풀 열기 (파일을 persistent heap으로 매핑 후 풀 핸들러 반환)
    pub fn open(filepath: &str) -> Result<&PoolHandle, Error> {
        // 1. 파일 열기 (파일이 존재하지 않는다면 실패)
        let file = OpenOptions::new().read(true).write(true).open(filepath)?;

        // 2. 메모리 매핑 후 글로벌 풀 세팅
        let mmap = unsafe { memmap::MmapOptions::new().map_mut(&file)? };
        global::init(PoolHandle {
            mmap,
            len: file.metadata()?.len() as usize,
        });
        // create시 초기화가 제대로 안된 풀이면 에러 반환
        if !global_pool().unwrap().pool().is_initialized {
            global::clear();
            return Err(Error::new(ErrorKind::InvalidData, "Invalid pool"));
        }

        // 3. 글로벌 풀의 핸들러 반환
        Ok(global_pool().unwrap())
    }

    /// 풀 닫기
    // TODO: 디자인 고민
    //  - file open/close API와 유사하게 input으로 받은 PoolHandle을 close하는 게 좋을지?
    //  - 그렇게 한다면, 어떻게?
    pub fn close() {
        // 메모리 매핑에 사용한 `MmapMut` 오브젝트가 글로벌 풀 내부의 `mmap` 필드에 저장되어있었다면 이때 매핑 해제됨
        global::clear();
    }

    /// 풀에 T의 크기만큼 할당 후 이를 가리키는 포인터 얻음
    fn alloc<T>(&self) -> PersistentPtr<T> {
        // TODO: 실제 allocator 사용 (현재는 base + 1024 위치에 할당된 것처럼 동작)
        // let addr_allocated = self.allocator.alloc(mem::size_of::<T>());
        let addr_allocated = 1024;
        PersistentPtr::from(addr_allocated)
    }

    /// persistent pointer가 가리키는 풀 내부의 메모리 블록 할당해제
    fn free<T>(&self, _pptr: &mut PersistentPtr<T>) {
        todo!("pptr이 가리키는 메모리 블록 할당해제")
    }
}

#[cfg(test)]
mod test {
    use env_logger as _;
    use log::{self as _, debug};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

    use crate::persistent::PersistentOp;
    use crate::plocation::pool::*;

    struct RootObj {
        // 단순 usize, bool이 아닌 Atomic을 사용하는 이유: `PersistentOp` trait이 &mut self를 받지 않기때문
        value: AtomicUsize,
        flag: AtomicBool,
    }

    impl Default for RootObj {
        fn default() -> Self {
            Self {
                value: AtomicUsize::new(0),
                flag: AtomicBool::new(false),
            }
        }
    }

    impl RootObj {
        /// invariant 검사(flag=1 => value=42)
        fn check_inv(&self, _client: &mut RootClient, _input: ()) -> Result<(), ()> {
            if self.flag.load(SeqCst) {
                debug!("check inv");
                assert_eq!(self.value.load(SeqCst), 42);
            } else {
                debug!("update");
                self.value.store(42, SeqCst);
                self.flag.store(true, SeqCst);
            }
            Ok(())
        }
    }

    impl PersistentOp<RootClient> for RootObj {
        type Input = ();
        type Output = Result<(), ()>;

        fn persistent_op(&self, client: &mut RootClient, input: Self::Input) -> Self::Output {
            self.check_inv(client, input)
        }
    }

    #[derive(Default)]
    struct RootClient {
        // 이 테스트는 간단한 예제이기 때문에 `RootClient` 필드가 비어있음
    // 그러나 만약 `RootObj`에 Queue가 들어간다면 Queue를 위한 Push/PopClient를 필드로 추가해야함
    }

    impl PersistentClient for RootClient {
        fn reset(&mut self) {
            // no op
        }
    }

    const FILE_NAME: &str = "test/check_inv.pool";
    const FILE_SIZE: usize = 8 * 1024;

    /// 언제 crash나든 invariant 보장함을 보이는 테스트: flag=1 => value=42
    #[test]
    fn check_inv() {
        // 커맨드에 RUST_LOG=debug 포함시 debug! 로그 출력
        env_logger::init();

        // 풀 없으면 새로 만듦
        let _ = Pool::create::<RootObj, RootClient>(FILE_NAME, FILE_SIZE).is_ok();

        // 풀 열고 루트 오브젝트, 루트 클라이언트 가져오기
        let pool_handle = Pool::open(FILE_NAME).unwrap();
        let mut root_ptr = pool_handle.get_root::<RootObj, RootClient>().unwrap();
        let (root_obj, root_client) = unsafe { root_ptr.deref_mut() };

        // 루트 클라이언트로 루트 오브젝트의 op 실행
        // 이 경우 루트 오브젝트의 op은 invariant 검사하는 `check_inv()`
        root_obj.persistent_op(root_client, ()).unwrap();
    }
}
