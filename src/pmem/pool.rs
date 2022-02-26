//! Persistent Memory Pool
//!
//! 파일을 persistent heap으로서 가상주소에 매핑하고, 그 메모리 영역을 관리하는 메모리 "풀"

use std::alloc::Layout;
use std::ffi::{c_void, CString};
use std::io::Error;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::{fs, mem};

use crate::ploc::{CASCheckpointArr, ExecInfo};
use crate::pmem::global::global_pool;
use crate::pmem::ll::persist_obj;
use crate::pmem::ptr::PPtr;
use crate::pmem::{global, ralloc::*};
use crate::*;
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::{thread, CachePadded};

// metadata, root obj, root memento들이 Ralloc의 몇 번째 root에 위치하는 지를 나타내는 상수
enum RootIdx {
    RootObj,       // root obj
    CASCheckpoint, // cas general checkpoint
    NrMemento,     // memento의 개수
    MementoStart,  // root memento(s) 시작 위치
}

/// 열린 풀을 관리하기 위한 풀 핸들러
///
/// # Safety
///
/// `Pool::create` 혹은 `Pool::open`으로 `PoolHandle`을 새로 얻을 시,
/// 이전에 사용하던 `PoolHandle`은 더이상 사용하면 안됨 (Ralloc이 global pool 하나만 사용하기 때문에, pool 정보가 덮어씌워짐)
///
/// # Example
///
/// ```no_run
/// # // "이렇게 사용한다"만 보이기 위해 파일을 실제로 만들진 않고 "no_run"으로 함
/// # use memento::pmem::pool::*;
/// # use memento::*;
/// # use memento::test_utils::tests::{DummyRootObj as MyRootObj, DummyRootMemento as MyRootMemento};
/// # use crossbeam_epoch::{self as epoch};
/// // 풀 생성 후 풀의 핸들러 얻기
/// let pool_handle = Pool::create::<MyRootObj, MyRootMemento>("foo.pool", 8 * 1024 * 1024 * 1024, 1).unwrap();
///
/// // 루트 memento 실행
/// pool_handle.execute::<MyRootObj, MyRootMemento>();
/// ```
#[derive(Debug)]
pub struct PoolHandle {
    /// 풀의 시작주소
    start: usize,

    /// 풀의 길이
    len: usize,

    /// Detectable execution information per thread
    pub(crate) exec_info: ExecInfo,
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

    /// 풀의 메인 프로그램 시작
    ///
    /// O: root obj
    /// M: root memento(s)
    pub fn execute<O, M>(&'static self)
    where
        O: RootObj<M> + Send + Sync,
        M: Collectable + Default + Send + Sync,
    {
        // root obj 얻기
        let root_obj = unsafe {
            (RP_get_root_c(RootIdx::RootObj as u64) as *const O)
                .as_ref()
                .unwrap()
        };

        // root memento(들)의 개수 얻기
        let nr_memento = unsafe { *(RP_get_root_c(RootIdx::NrMemento as u64) as *mut usize) };

        #[allow(box_pointers)]
        thread::scope(|scope| {
            // tid번째 스레드가 tid번째 memento를 성공할때까지 반복
            let barrier = Arc::new(std::sync::Barrier::new(nr_memento));

            for tid in 1..=nr_memento {
                // tid번째 root memento 얻기
                let m_addr =
                    unsafe { RP_get_root_c(RootIdx::MementoStart as u64 + tid as u64) as usize };
                let barrier = barrier.clone();

                let _ = scope.spawn(move |_| {
                    let started = AtomicBool::new(false);
                    thread::scope(|scope| {
                        loop {
                            self.exec_info.local_max_time[tid].store(0, Ordering::Relaxed);

                            // memento 실행
                            let handler = scope.spawn(|_| {
                                let root_mmt = unsafe { (m_addr as *mut M).as_mut().unwrap() };

                                let guard = unsafe { epoch::old_guard(tid) };

                                if !started.load(Ordering::Relaxed) {
                                    started.store(true, Ordering::Relaxed);
                                    let _ = barrier.wait();
                                }

                                let _ = root_obj.run(root_mmt, tid, &guard, self);
                            });

                            // 성공시 종료, 실패(i.e. crash)시 memento 재실행
                            // 실패시 사용하던 guard도 정리하지 않음. 주인을 잃은 guard는 다음 반복에서 생성된 thread가 이어서 잘 사용해야함
                            match handler.join() {
                                Ok(_) => break,
                                Err(_) => println!("PANIC: Root memento No.{} re-executed.", tid),
                            }
                        }
                    })
                    .unwrap();
                });
            }
        })
        .unwrap();
    }

    /// unsafe get root
    ///
    /// It is useful to check the object in the pool directly
    ///
    /// # Safety
    ///
    /// TODO
    // PiBench로 실험 돌릴 때 PiBench가 사용할 root obj, root memento를 세팅하기 위해 필요
    pub unsafe fn get_root(&self, ix: u64) -> *mut c_void {
        RP_get_root_c(ix)
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
}

impl Drop for PoolHandle {
    fn drop(&mut self) {
        unsafe { RP_close() }
    }
}

/// 풀 열기/닫기 및 메타데이터를 관리하는 역할
#[derive(Debug)]
pub struct Pool {
    // NOTE:
// - Ralloc의 Pool management API를 사용하기 때문에 Pool에 위치할 메타데이터를 추가하려면 Ralloc의 set/get root API를 써야함
// - Ralloc의 default 설정은 1024개의 root를 사용하며, 이는 pm_config.hpp의 `MAX_ROOT`로 조절 가능
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
    pub fn create<O, M>(
        filepath: &str,
        size: usize,
        nr_memento: usize, // Root Memento의 개수
    ) -> Result<&'static PoolHandle, Error>
    where
        O: RootObj<M>,
        M: Collectable + Default,
    {
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

        unsafe {
            // general cas checkpoint 세팅
            let cas_chk_arr = RP_malloc(mem::size_of::<[CASCheckpointArr; 2]>() as u64)
                as *mut [CASCheckpointArr; 2];
            cas_chk_arr.write(array_init::array_init(|_| {
                array_init::array_init(|_| CachePadded::new(AtomicU64::new(0)))
            }));
            persist_obj(cas_chk_arr.as_mut().unwrap(), true);
            let _prev = RP_set_root(cas_chk_arr as *mut c_void, RootIdx::CASCheckpoint as u64);
            let chk_ref = cas_chk_arr.as_ref().unwrap();

            // 글로벌 pool 세팅
            global::init(PoolHandle {
                start: RP_mmapped_addr(),
                len: size,
                exec_info: ExecInfo::from(chk_ref),
            });

            let pool = global_pool().unwrap();

            // metadta, root obj, root memento 세팅
            // root obj 세팅
            let o_ptr = RP_malloc(mem::size_of::<O>() as u64) as *mut O;
            o_ptr.write(O::pdefault(pool));
            persist_obj(o_ptr.as_mut().unwrap(), true);
            let _prev = RP_set_root(o_ptr as *mut c_void, RootIdx::RootObj as u64);

            // root memento의 개수 세팅
            let nr_memento_ptr = RP_malloc(mem::size_of::<usize>() as u64) as *mut usize;
            nr_memento_ptr.write(nr_memento);
            persist_obj(nr_memento_ptr.as_mut().unwrap(), true);
            let _prev = RP_set_root(nr_memento_ptr as *mut c_void, RootIdx::NrMemento as u64);

            // root memento(들) 세팅: 1 ~ nr_memento
            for i in 1..nr_memento + 1 {
                let root_ptr = RP_malloc(mem::size_of::<M>() as u64) as *mut M;
                root_ptr.write(M::default());
                persist_obj(root_ptr.as_mut().unwrap(), true);
                let _prev = RP_set_root(
                    root_ptr as *mut c_void,
                    RootIdx::MementoStart as u64 + i as u64,
                );
            }

            Ok(pool)
        }
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
    pub unsafe fn open<O, M>(filepath: &str, size: usize) -> Result<&'static PoolHandle, Error>
    where
        O: RootObj<M>,
        M: Collectable + Default,
    {
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
        let chk_ref = (RP_get_root_c(RootIdx::CASCheckpoint as u64)
            as *const [CASCheckpointArr; 2])
            .as_ref()
            .unwrap();

        global::init(PoolHandle {
            start: RP_mmapped_addr(),
            len: size,
            exec_info: ExecInfo::from(chk_ref),
        });

        // GC 수행
        {
            unsafe extern "C" fn root_filter<T: Collectable>(
                ptr: *mut ::std::os::raw::c_char,
                tid: usize,
                gc: &mut GarbageCollection,
            ) {
                RP_mark(
                    gc,
                    ptr,
                    tid.wrapping_sub(RootIdx::MementoStart as usize),
                    Some(T::filter_inner),
                );
            }

            // root obj의 filter func 등록
            RP_set_root_filter(Some(root_filter::<O>), RootIdx::RootObj as u64);

            // root memento(들)의 filter func 등록
            let nr_memento = *(RP_get_root_c(RootIdx::NrMemento as u64) as *mut usize);
            for tid in 1..nr_memento + 1 {
                // root memento들은 Ralloc의 2번째 root부터 위치
                RP_set_root_filter(
                    Some(root_filter::<M>),
                    RootIdx::MementoStart as u64 + tid as u64,
                );
            }

            // GC 호출
            //
            // NOTE: Ralloc의 API상 이전에 RP_close로 잘 닫고 끝났었다면(i.e. crash가 아니면) GC 호출해도 수행되지 않음
            //
            // NOTE: Ralloc의 `IX_NR_MEM` 번째 root는 filter func을 등록하지 않았으니 GC 수행시 default filter가 돔
            //       그렇지만 우리 로직에선 안전함. `nr_memento` 값을 주소로 보고 marking 시도하는데, marking 하려는 (절대)주소가
            //       pool 영역이 아니면 marking 되지않고 무시됨. `nr_memento` 값이 pool 영역 범위 내의 값이 될 확률은 매우 적음
            let _is_gc_executed = RP_recover();
        }

        let pool = global_pool().unwrap();
        pool.exec_info.set_info();

        // 글로벌 풀의 핸들러 반환
        Ok(pool)
    }

    /// TODO(doc)
    pub fn remove(filepath: &str) -> Result<(), Error> {
        fs::remove_file(&(filepath.to_owned() + "_basemd"))?;
        fs::remove_file(&(filepath.to_owned() + "_desc"))?;
        fs::remove_file(&(filepath.to_owned() + "_sb"))?;
        Ok(())
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

/// Root object of pool
pub trait RootObj<M: Collectable + Default>: PDefault + Collectable {
    /// Root object's default run function with a root memento
    fn run(&self, mmt: &mut M, tid: usize, guard: &Guard, pool: &PoolHandle);
}

#[cfg(test)]
mod tests {
    use crossbeam_epoch::Guard;
    use env_logger as _;
    use log::{self as _, debug};

    use crate::pmem::pool::*;
    use crate::test_utils::tests::*;

    impl RootObj<CheckInv> for DummyRootObj {
        fn run(&self, mmt: &mut CheckInv, _: usize, _: &Guard, _: &PoolHandle) {
            if mmt.flag {
                debug!("check inv");
                assert_eq!(mmt.value, 42);
            } else {
                debug!("update");
                mmt.value = 42;
                mmt.flag = true;
            }
        }
    }

    #[derive(Default)]
    struct CheckInv {
        value: usize,
        flag: bool,
    }

    impl Collectable for CheckInv {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
            // no-op
        }
    }

    const FILE_NAME: &str = "check_inv.pool";
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // TODO: root op 실행 로직 고치기 https://cp-git.kaist.ac.kr/persistent-mem/memento/-/issues/95
    /// 언제 crash나든 invariant 보장함을 보이는 테스트: flag=1 => value=42
    #[test]
    fn check_inv() {
        // 커맨드에 RUST_LOG=debug 포함시 debug! 로그 출력
        env_logger::init();

        run_test::<DummyRootObj, CheckInv, _>(FILE_NAME, FILE_SIZE, 1);
    }
}
