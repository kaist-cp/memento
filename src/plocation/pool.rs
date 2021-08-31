//! Persistent Memory Pool
//!
//! 파일을 persistent heap으로서 가상주소에 매핑하고, 그 메모리 영역을 관리하는 메모리 "풀"

use memmap::*;
use std::alloc::Layout;
use std::fs::OpenOptions;
use std::io::Error;
use std::mem;
use std::path::Path;
use tempfile::*;

use crate::persistent::*;
use crate::plocation::ptr::PPtr;

/// 열린 풀을 관리하기 위한 풀 핸들러
///
/// # Example
///
/// ```no_run
/// # // "이렇게 사용한다"만 보이기 위해 불필요한 정보는 숨기고 "no_run"으로 함
/// #
/// # use compositional_persistent_object::plocation::pool::*;
/// # use compositional_persistent_object::persistent::*;
/// #
/// # #[derive(Default)]
/// # struct MyRootOp {}
/// #
/// # impl POp for MyRootOp {
/// #     type Object = ();
/// #     type Input = ();
/// #     type Output = Result<(), ()>;
/// #
/// #     fn run(&mut self, _: &Self::Object, _: Self::Input) -> Self::Output {
/// #         Ok(())
/// #     }
/// #
/// #     fn reset(&mut self, _: bool) {}
/// # }
///
/// // 풀 생성 후 풀의 핸들러 얻기
/// let pool_handle = Pool::create::<MyRootOp>("foo.pool", 8 * 1024).unwrap();
///
/// // 핸들러로 풀의 루트 Op 가져오기
/// let mut root_ptr = pool_handle.get_root::<MyRootOp>().unwrap();
/// let root_op = unsafe { root_ptr.deref_mut(&pool_handle) };
///
/// // 루트 Op 실행
/// root_op.run(&(), ()).unwrap();
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

    /// 절대주소를 상대주소로 변환.
    // TODO: valid아니면 0으로라도 넘겨야함. volatile에 만든 Pop::default로 바로 검사하는 게 동작하기 위해서.
    #[inline]
    pub fn get_persistent_addr(&self, raw: usize) -> Option<usize> {
        if self.valid(raw) {
            return Some(raw - self.start());
        }
        None
    }

    /// 풀의 루트 Op을 가리키는 포인터 반환
    #[inline]
    pub fn get_root<O: POp>(&self) -> Result<PPtr<O>, Error> {
        // TODO: 잘못된 타입으로 가져오려하면 에러 반환
        Ok(PPtr::from(self.pool().root_offset))
    }

    /// 풀에 T의 크기만큼 할당 후 이를 가리키는 포인터 얻음
    #[inline]
    pub fn alloc<T>(&self) -> PPtr<T> {
        self.pool().alloc::<T>()
    }

    /// 풀에 Layout에 맞게 할당 후 이를 T로 가리키는 포인터 반환
    ///
    /// # Safety
    ///
    /// TODO
    #[inline]
    pub unsafe fn alloc_layout<T>(&self, layout: Layout) -> PPtr<T> {
        self.pool().alloc_layout(layout)
    }

    /// persistent pointer가 가리키는 풀 내부의 메모리 블록 할당해제
    #[inline]
    pub fn free<T>(&self, pptr: PPtr<T>) {
        self.pool().free(pptr)
    }

    /// offset 주소부터 Layout 크기만큼 할당 해제
    ///
    /// # Safety
    ///
    /// TODO
    #[inline]
    pub unsafe fn free_layout(&self, offset: usize, layout: Layout) {
        self.pool().free_layout(offset, layout)
    }

    #[inline]
    fn pool(&self) -> &Pool {
        unsafe { &*(self.start() as *const Pool) }
    }

    #[inline]
    /// 절대주소가 풀에 속한 주소인지 확인
    fn valid(&self, raw: usize) -> bool {
        raw >= self.start() && raw < self.end()
    }
}

/// 풀 열기/닫기 및 메타데이터를 관리하는 역할
///
/// # Pool Address Layout
///
/// ```test
/// [ metadata | root op |       ...        ]
/// ^ base     ^ base + root offset                         ^ end
/// ```
#[derive(Debug)]
pub struct Pool {
    /// 풀의 시작주소로부터 루트 Op까지의 거리
    root_offset: usize,
    // TODO: 풀의 메타데이터는 여기에 필드로 추가
}

impl Pool {
    /// 풀 생성
    ///
    /// 풀로서 사용할 파일을 생성, 초기화(풀 레이아웃에 맞게 내부구조 초기화)한 후 풀의 핸들러 반환
    ///
    /// # Errors
    ///
    /// * `filepath`에 파일이 이미 존재한다면 실패
    // TODO: filepath의 타입이 `P: AsRef<Path>`이면 좋겠다. 그런데 이러면 generic P에 대한 type inference가 안돼서 사용자가 `Pool::create::<RootOp, &str>("foo.pool")`처럼 호출해야함. 이게 괜찮나?
    pub fn create<O: POp>(filepath: &str, size: usize) -> Result<PoolHandle, Error> {
        // 초기화 도중의 crash를 고려하여,
        //   1. 임시파일로서 풀을 초기화 한 후
        //   2. 초기화가 완료되면 "filepath"로 옮김

        // # 임시파일 생성
        let pmem_path = Path::new(filepath).parent().unwrap(); // pmem mounted directory
        std::fs::create_dir_all(pmem_path)?; // e.g. "a/b/c.pool"라면, a/b/ 폴더도 만들어줌
        let temp_file = NamedTempFile::new_in(pmem_path.as_os_str())?; // 임시파일 또한 pmem mount된 경로에서 생성돼야함
        let file = temp_file.as_file();

        // # 임시파일을 풀 레이아웃에 맞게 초기화
        file.set_len(size as u64)?;
        let mmap = unsafe { memmap::MmapOptions::new().map_mut(file)? };
        let start = mmap.as_ptr() as usize;
        let pool = unsafe { &mut *(start as *mut Pool) };

        // 메타데이터 초기화
        pool.root_offset = mem::size_of::<Pool>(); // e.g. 메타데이터 크기(size_of::<Pool>)가 16이라면, 루트는 풀의 시작주소+16에 위치

        // 루트 Op 초기화
        let root_op = unsafe { &mut *((start + pool.root_offset) as *mut O) };
        *root_op = O::default();

        // # 초기화된 임시파일을 "filepath"로 옮기기
        // TODO: filepath에 파일이 이미 존재하면 여기서 실패하는데, 이를 위에서 ealry return할지 고민하기
        let _ = temp_file.persist_noclobber(filepath)?;

        // # 생성한 파일을 풀로서 open
        Self::open(filepath)
    }

    /// 풀 열기
    ///
    /// 파일을 persistent heap으로 매핑 후 풀을 다룰 수 있는 핸들러를 반환함
    ///
    /// # Errors
    ///
    /// * `filepath`에 파일이 존재하지 않는다면 실패
    pub fn open<P: AsRef<Path>>(filepath: P) -> Result<PoolHandle, Error> {
        // 파일 열기
        let file = OpenOptions::new().read(true).write(true).open(filepath)?;

        // 메모리 매핑 후 풀의 핸들러 반환
        Ok(PoolHandle {
            mmap: unsafe { memmap::MmapOptions::new().map_mut(&file)? },
            len: file.metadata()?.len() as usize,
        })
    }

    /// 풀에 T의 크기만큼 할당 후 이를 가리키는 포인터 반환
    fn alloc<T>(&self) -> PPtr<T> {
        // TODO: 실제 allocator 사용 (현재는 base + 1024 위치에 할당된 것처럼 동작)
        // let addr_allocated = self.allocator.alloc(mem::size_of::<T>());
        let addr_allocated = 1024;
        PPtr::from(addr_allocated)
    }

    /// 풀에 Layout에 맞게 할당 후 이를 T로 가리키는 포인터 반환
    ///
    /// - `PersistentPtr<T>`가 가리킬 데이터의 크기를 정적으로 알 수 없을 때, 할당할 크기(`Layout`)를 직접 지정하기 위해 필요
    /// - e.g. dynamically sized slices
    unsafe fn alloc_layout<T>(&self, _layout: Layout) -> PPtr<T> {
        // TODO: 실제 allocator 사용 (현재는 base + 1024 위치에 할당된 것처럼 동작)
        let addr_allocated = 1024;
        PPtr::from(addr_allocated)
    }

    /// persistent pointer가 가리키는 풀 내부의 메모리 블록 할당해제
    fn free<T>(&self, _pptr: PPtr<T>) {
        todo!("pptr이 가리키는 메모리 블록 할당해제")
    }

    /// offset 주소부터 Layout 크기만큼 할당 해제
    ///
    /// - `PersistentPtr<T>`가 가리키는 데이터의 크기를 정적으로 알 수 없을때, 할당 해제할 크기(`Layout`)를 직접 지정하기 위해 필요
    /// - e.g. dynamically sized slices
    unsafe fn free_layout(&self, _offset: usize, _layout: Layout) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use env_logger as _;
    use log::{self as _, debug};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

    use crate::persistent::POp;
    use crate::plocation::pool::*;
    use crate::utils::tests::*;

    #[derive(Default)]
    struct RootOp {
        // 단순 usize, bool이 아닌 Atomic을 사용하는 이유: `PersistentOp` trait이 &mut self를 받지 않기때문
        value: AtomicUsize,
        flag: AtomicBool,
    }

    impl POp for RootOp {
        type Object = ();
        type Input = ();
        type Output = Result<(), ()>;

        // invariant 검사(flag=1 => value=42)
        fn run(&mut self, _: &Self::Object, _: Self::Input, _: &PoolHandle) -> Self::Output {
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

        fn reset(&mut self, _: bool) {
            // no-op
        }
    }

    const FILE_NAME: &str = "check_inv.pool";
    const FILE_SIZE: usize = 8 * 1024;

    /// 언제 crash나든 invariant 보장함을 보이는 테스트: flag=1 => value=42
    #[test]
    fn check_inv() {
        // 커맨드에 RUST_LOG=debug 포함시 debug! 로그 출력
        env_logger::init();
        let filepath = get_test_path(FILE_NAME);

        // 풀 열기 (없으면 새로 만듦)
        let pool_handle = Pool::open(&filepath)
            .unwrap_or_else(|_| Pool::create::<RootOp>(&filepath, FILE_SIZE).unwrap());

        // 루트 Op 가져오기
        let mut root_ptr = pool_handle.get_root::<RootOp>().unwrap();
        let root_op = unsafe { root_ptr.deref_mut(&pool_handle) };

        // 루트 Op 실행. 이 경우 루트 Op은 invariant 검사(flag=1 => value=42)
        root_op.run(&(), (), &pool_handle).unwrap();
    }
}
