//! Persistent Memory Pool
//!
//! 파일을 persistent heap으로서 가상주소에 매핑하고, 그 메모리 영역을 관리하는 메모리 "풀"

use std::alloc::Layout;
use std::ffi::{c_void, CString};
use std::io::Error;
use std::path::Path;
use std::{fs, mem};

use crate::persistent::*;
use crate::plocation::global::global_pool;
use crate::plocation::ptr::PPtr;
use crate::plocation::{global, ralloc::*};

/// 열린 풀을 관리하기 위한 풀 핸들러
///
/// # Safety
///
/// `Pool::create` 혹은 `Pool::open`으로 `PoolHandle`을 새로 얻을 시, 이전에 사용하던 `PoolHandle`은 더이상 사용하면 안됨 (Ralloc이 global pool 하나만 사용하기 때문에, pool 정보가 덮어씌워짐)
///
/// # Example
///
/// ```no_run
/// # // "이렇게 사용한다"만 보이기 위해 파일을 실제로 만들진 않고 "no_run"으로 함
/// # use compositional_persistent_object::plocation::pool::*;
/// # use compositional_persistent_object::persistent::*;
/// # use compositional_persistent_object::utils::tests::DummyRootOp as MyRootOp;
/// # use crossbeam_epoch::{self as epoch};
/// // 풀 생성 후 풀의 핸들러 얻기
/// let pool_handle = Pool::create::<MyRootOp>("foo.pool", 8 * 1024 * 1024 * 1024).unwrap();
///
/// // 핸들러로 풀의 루트 Op 가져오기
/// let root_op = pool_handle.get_root::<MyRootOp>();
///
/// // 루트 Op 실행
/// let mut guard = epoch::pin();
/// root_op.run((), (), &mut guard, &pool_handle).unwrap();
/// ```
#[derive(Debug)]
pub struct PoolHandle {
    /// 풀의 시작주소
    start: usize,

    /// 풀의 길이
    len: usize,

    /// recovery 진행중 여부
    recovering: bool,
}

impl PoolHandle {
    /// 풀의 시작주소 반환
    #[inline]
    pub fn start(&self) -> usize {
        self.start
    }

    /// 풀의 끝주소 반환
    #[inline]
    pub fn end(&self) -> usize {
        self.start() + self.len
    }

    /// 풀의 루트 Op을 가리키는 포인터 반환
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub fn get_root<O: Memento>(&self) -> &mut O {
        // NOTE: Ralloc은 1024개의 root를 set/get할 수 있는데, 우리는 0번째만 사용
        let root_ptr = unsafe { RP_get_root_c(0) } as *mut O;
        unsafe { &mut *root_ptr }
    }

    /// 풀에 T의 크기만큼 할당 후 이를 가리키는 포인터 얻음
    #[inline]
    pub fn alloc<T>(&self) -> PPtr<T> {
        let ptr = self.pool().alloc(mem::size_of::<T>());
        PPtr::from(ptr as usize - self.start())
    }

    /// 풀에 Layout에 맞게 할당 후 이를 T로 가리키는 포인터 반환
    ///
    /// - `PersistentPtr<T>`가 가리킬 데이터의 크기를 정적으로 알 수 없을 때, 할당할 크기(`Layout`)를 직접 지정하기 위해 필요
    /// - e.g. dynamically sized slices
    ///
    /// # Safety
    ///
    /// TODO
    #[inline]
    pub unsafe fn alloc_layout<T>(&self, layout: Layout) -> PPtr<T> {
        let ptr = self.pool().alloc(layout.size());
        PPtr::from(ptr as usize - self.start())
    }

    /// persistent pointer가 가리키는 풀 내부의 메모리 블록 할당해제
    #[inline]
    pub fn free<T>(&self, pptr: PPtr<T>) {
        let addr_abs = self.start() + pptr.into_offset();
        self.pool().free(addr_abs as *mut u8);
    }

    /// offset 주소부터 Layout 크기만큼 할당 해제
    ///
    /// - `PersistentPtr<T>`가 가리키는 데이터의 크기를 정적으로 알 수 없을때, 할당 해제할 크기(`Layout`)를 직접 지정하기 위해 필요
    /// - e.g. dynamically sized slices
    ///
    /// # Safety
    ///
    /// TODO
    #[inline]
    pub unsafe fn free_layout(&self, offset: usize, _layout: Layout) {
        // NOTE: Ralloc의 free는 size를 받지 않지 않으므로 할당해제할 주소만 잘 넘겨주면 됨
        let addr_abs = self.start() + offset;
        self.pool().free(addr_abs as *mut u8);
    }

    #[inline]
    fn pool(&self) -> &Pool {
        unsafe { &*(self.start() as *const Pool) }
    }

    /// 절대주소가 풀에 속한 주소인지 확인
    #[inline]
    pub fn valid(&self, raw: usize) -> bool {
        raw >= self.start() && raw < self.end()
    }

    /// 현재 recovery 중인지 여부 확인
    #[inline]
    pub fn is_recovering(&self) -> bool {
        self.recovering
    }
}

impl Drop for PoolHandle {
    fn drop(&mut self) {
        unsafe { RP_close() }
    }
}

/// 풀 열기/닫기 및 메타데이터를 관리하는 역할
#[derive(Debug)]
pub struct Pool {
    // Ralloc의 API를 사용하기 때문에 별다른 필드 필요없음
}

impl Pool {
    /// 풀 생성
    ///
    /// 풀로서 사용할 파일을 생성, 초기화(풀 레이아웃에 맞게 내부구조 초기화)한 후 풀의 핸들러 반환
    ///
    /// # Errors
    ///
    /// * `filepath`에 파일이 이미 존재한다면 실패
    /// * `size`를 `1GB` 이상, `1TB` 이하로 하지 않는다면 실패 (Ralloc 내부의 assert문에 의해 강제)
    //
    // TODO: filepath 타입을 `P: AsRef<Path>`로 하기
    // - <O: Memento, P: AsRef<Path>>로 받아도 잘 안됨. 이러면 generic P에 대한 type inference가 안돼서 사용자가 `O`, `P`를 둘다 명시해줘야함 (e.g. Pool::open::<RootOp, &str>("foo.pool") 처럼 호출해야함)
    pub fn create<O: Memento>(filepath: &str, size: usize) -> Result<&'static PoolHandle, Error> {
        // 파일 이미 있으면 에러 반환
        // - Ralloc의 init은 filepath에 postfix("_based", "_desc", "_sb")를 붙여 파일을 생성하기 때문에, 그 중 하나인 "_basemd"를 붙여 확인
        if Path::new(&(filepath.to_owned() + "_basemd")).exists() {
            return Err(Error::new(
                std::io::ErrorKind::AlreadyExists,
                "File already exist.",
            ));
        }
        fs::create_dir_all(Path::new(filepath).parent().unwrap())?;

        // 새로 init하기 전에 이전에 열었던 pool을 미리 clear
        // - RP_init으로 Ralloc에 새로 세팅된 정보가 이전에 사용하던 PoolHandle의 drop으로 RP_close되면 안됨
        // - 따라서 이전에 사용하던 것 미리 drop
        global::clear();

        // 파일 만들고 Ralloc의 pool format으로 초기화
        let filepath = CString::new(filepath).expect("CString::new failed");
        let is_reopen = unsafe { RP_init(filepath.as_ptr(), size as u64) };
        assert_eq!(is_reopen, 0);

        // root로 사용할 obj를 만든 후 root로 세팅
        // NOTE: Ralloc은 1024개의 root를 set/get할 수 있는데, 우리는 0번째만 사용
        let root_ptr = unsafe { RP_malloc(mem::size_of::<O>() as u64) as *mut O };
        unsafe { root_ptr.write(O::default()) };
        let _prev = unsafe { RP_set_root(root_ptr as *mut c_void, 0) };

        // 매핑된 주소의 시작주소를 얻고 글로벌 pool 세팅
        let start = unsafe {
            let mut start: *mut i32 = std::ptr::null_mut();
            let mut end: *mut i32 = std::ptr::null_mut();
            let _ret = RP_region_range(
                1,
                &mut start as *mut *mut _ as *mut *mut c_void,
                &mut end as *mut *mut _ as *mut *mut c_void,
            );
            start as usize
        };
        global::init(PoolHandle {
            start,
            len: size,
            recovering: true,
        });

        // 글로벌 풀의 핸들러 반환
        Ok(global_pool().unwrap())
    }

    /// 풀 열기
    ///
    /// 파일을 persistent heap으로 매핑 후, 루트타입 `O`를 가진 풀의 핸들러 반환
    ///
    /// # Safety
    ///
    /// * `Pool::create`시 지정한 root op 타입(i.e. `O`)과 같은 타입으로 불러와야함
    ///
    /// # Errors
    ///
    /// * `filepath`에 파일이 존재하지 않는다면 실패
    /// * `Pool::create`시 지정한 size와 같은 크기로 호출하지 않으면 실패 (Ralloc 내부의 assert문에 의해 강제)
    // TODO: `size` 의문. Ralloc의 RP_init은 왜 풀 열 때에도 처음 만들때랑 같은 size로 호출해야하나?
    //
    // TODO: `size` 안받게 할지 고민. 파일 크기로 `Pool::create`시 지정한 size를 역계산하여 사용?
    // - `Pool::create`시 지정한 size랑 실제 생성되는 파일 크기는 다름. 8GB로 create 했어도, 파일 크기는 Ralloc의 로직에 따라 계산된 8GB+a로 됨
    // - 8GB+a에서 Ralloc의 로직을 역계산하여 8GB를 알아낼 수 있을듯
    //
    // TODO: filepath 타입을 `P: AsRef<Path>`로 하기
    // - <O: Memento, P: AsRef<Path>>로 받아도 잘 안됨. 이러면 generic P에 대한 type inference가 안돼서 사용자가 `O`, `P`를 둘다 명시해줘야함 (e.g. Pool::open::<RootOp, &str>("foo.pool") 처럼 호출해야함)
    pub unsafe fn open<O: Memento>(
        filepath: &str,
        size: usize,
    ) -> Result<&'static PoolHandle, Error> {
        // 파일 없으면 에러 반환
        // - "_basemd"를 붙여 확인하는 이유: Ralloc의 init은 filepath에 postfix("_based", "_desc", "_sb")를 붙여 파일을 생성
        if !Path::new(&(filepath.to_owned() + "_basemd")).exists() {
            return Err(Error::new(std::io::ErrorKind::NotFound, "File not found."));
        }

        // 새로 열기 전에 이전에 열었던 pool을 미리 clear
        // - RP_init으로 Ralloc에 새로 세팅된 정보가 이전에 사용하던 PoolHandle의 drop으로 RP_close되면 안됨
        // - 따라서 이전에 사용하던 것 미리 drop
        global::clear();

        // pool 파일 열기
        let filepath = CString::new(filepath).expect("CString::new failed");
        let is_reopen = RP_init(filepath.as_ptr(), size as u64);
        assert_eq!(is_reopen, 1);

        // 매핑된 주소의 시작주소를 얻고 글로벌 pool 세팅
        let start = {
            let mut start: *mut i32 = std::ptr::null_mut();
            let mut end: *mut i32 = std::ptr::null_mut();
            let _ret = RP_region_range(
                1,
                &mut start as *mut *mut _ as *mut *mut c_void,
                &mut end as *mut *mut _ as *mut *mut c_void,
            );
            start as usize
        };
        global::init(PoolHandle {
            start,
            len: size,
            recovering: true,
        });

        // GC의 시작점을 등록하고 GC 수행
        // - 그러나 이전에 RP_close로 잘 닫았다면(i.e. crash가 아니면) 수행되지 않음
        unsafe extern "C" fn root_filter<O: Memento>(
            ptr: *mut ::std::os::raw::c_char,
            gc: &mut GarbageCollection,
        ) {
            RP_mark(gc, ptr, Some(O::filter_inner));
        }
        RP_set_root_filter(Some(root_filter::<O>), 0);
        let _is_gc_executed = RP_recover();

        // 글로벌 풀의 핸들러 반환
        Ok(global_pool().unwrap())
    }

    /// 풀에 size만큼 할당 후 이를 가리키는 포인터 반환
    #[inline]
    fn alloc(&self, size: usize) -> *mut u8 {
        let addr_abs = unsafe { RP_malloc(size as u64) };
        addr_abs as *mut u8
    }

    /// ptr이 가리키는 풀의 메모리 블록 할당해제
    #[inline]
    fn free(&self, ptr: *mut u8) {
        unsafe { RP_free(ptr as *mut c_void) }
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_epoch::{self as epoch, Guard};
    use env_logger as _;
    use log::{self as _, debug};
    use serial_test::serial;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

    use crate::persistent::Memento;
    use crate::plocation::pool::*;
    use crate::utils::tests::*;

    #[derive(Default)]
    struct RootOp {
        // 단순 usize, bool이 아닌 Atomic을 사용하는 이유: `PersistentOp` trait이 &mut self를 받지 않기때문
        value: AtomicUsize,
        flag: AtomicBool,
    }

    impl Collectable for RootOp {
        fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
            // no-op
        }
    }

    impl Memento for RootOp {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = !;

        fn run<'o>(
            &'o mut self,
            _: Self::Object<'o>,
            _: Self::Input,
            _: &mut Guard,
            _: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
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

        fn reset(&mut self, _: &mut Guard, _: bool, _: &'static PoolHandle) {
            // no-op
        }
    }

    const FILE_NAME: &str = "check_inv.pool";
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    /// 언제 crash나든 invariant 보장함을 보이는 테스트: flag=1 => value=42
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    // TODO: root op 실행 로직 고치기 https://cp-git.kaist.ac.kr/persistent-mem/memento/-/issues/95
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn check_inv() {
        // 커맨드에 RUST_LOG=debug 포함시 debug! 로그 출력
        env_logger::init();
        let filepath = get_test_abs_path(FILE_NAME);

        // 풀 열기 (없으면 새로 만듦)
        let pool_handle = unsafe { Pool::open::<RootOp>(&filepath, FILE_SIZE) }
            .unwrap_or_else(|_| Pool::create::<RootOp>(&filepath, FILE_SIZE).unwrap());

        // 루트 Op 가져오기
        let root_op = pool_handle.get_root::<RootOp>();

        // 루트 Op 실행. 이 경우 루트 Op은 invariant 검사(flag=1 => value=42)
        let mut guard = epoch::pin();
        root_op.run((), (), &mut guard, &pool_handle).unwrap();
    }
}
