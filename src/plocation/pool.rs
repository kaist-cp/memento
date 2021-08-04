//! Persistent Memory Pool
//!
//! 파일을 persistent heap으로서 가상주소에 매핑하고, 그 메모리 영역을 관리하는 메모리 "풀"

use std::fs::OpenOptions;
use std::io::Error;
use std::mem;

use crate::persistent::{PersistentClient, PersistentOpMut};

use super::global::{self, global_pool};
use super::ptr::PersistentPtr;
use memmap::*;

/// 열린 풀을 관리하기 위한 풀 핸들러
///
/// # 풀을 열고 사용하는 예시
///
/// ```
/// use std::fs::remove_file;
/// use compositional_persistent_object::plocation::pool::Pool;
///
/// // (1) 기존의 파일은 제거하고 풀 파일 새로 생성
/// let _ = remove_file("foo.pool");
/// let _ = Pool::create::<i32>("foo.pool", 8 * 1024);
///
/// // (2) 풀 열기
/// let pool_handle = Pool::open("foo.pool").unwrap();
///
/// // (3) 루트 오브젝트를 가져와서 사용
/// let mut head = pool_handle.get_root::<i32>().unwrap();
/// let mut head = unsafe { head.deref_mut() };
/// *head = 5;
/// assert_eq!(*head, 5);
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

    /// 풀의 루트 오브젝트를 가리키는 포인터 반환
    #[inline]
    pub fn get_root<O: PersistentOpMut<C>, C: PersistentClient>(
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

/// 풀의 내부를 관리하고 풀을 열고/닫기 위한 역할
///
/// # Pool Address Layout
///
/// [ metadata | root object |            ...               ]
/// ^ base     ^ base + root offset                         ^ end
#[derive(Debug)]
pub struct Pool {
    /// 풀의 시작주소로부터 루트 오브젝트까지의 거리
    root_offset: usize,
    // TODO: 풀의 메타데이터는 여기에 필드로 추가
}

impl Pool {
    /// 메타데이터 초기화
    fn init(&mut self) {
        // e.g. 메타데이터 크기(size_of::<Pool>)가 16이라면, 루트 오브젝트는 풀의 시작주소+16에 위치
        self.root_offset = mem::size_of::<Pool>();

        // TODO: 루트 오브젝트 초기화를 여기서하고, 메타데이터 초기화가 잘 완료됐는지는 나타내는 플래그 사용하기
        // ... init root object
        // self.is_initialized = true;
    }

    /// 풀 생성: 풀로서 사용할 파일을 생성하고 풀 레이아웃에 맞게 파일의 내부구조 초기화
    pub fn create<O: PersistentOpMut<C>, C: PersistentClient>(
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
        pool.init();
        Ok(())
    }

    /// 풀 열기: 파일을 persistent heap으로 매핑 후 풀 핸들러 반환
    pub fn open(filepath: &str) -> Result<&PoolHandle, Error> {
        // 1. 파일 열기 (파일이 존재하지 않는다면 실패)
        let file = OpenOptions::new().read(true).write(true).open(filepath)?;

        // 2. 메모리 매핑 후 글로벌 풀 세팅
        let mmap = unsafe { memmap::MmapOptions::new().map_mut(&file)? };
        global::init(PoolHandle {
            mmap,
            len: file.metadata()?.len() as usize,
        });

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
mod test_simple {
    use crate::persistent::PersistentOpMut;
    use crate::plocation::pool::*;
    use env_logger as _;
    use log::{self as _, debug};

    struct RootObj {
        value: usize,
        flag: bool,
    }

    impl RootObj {
        fn check_inv(&mut self, _client: &mut RootClient, _input: ()) -> Result<(), ()> {
            // invariant 검사(flag=1 => value=42)
            if self.flag {
                debug!("check invariant");
                assert_eq!(self.value, 42);
            } else {
                debug!("update");
                self.value = 42;
                self.flag = true;
            }
            Ok(())
        }
    }

    impl PersistentOpMut<RootClient> for RootObj {
        type Input = ();
        type Output = Result<(), ()>;

        fn persistent_op_mut(
            &mut self,
            client: &mut RootClient,
            input: Self::Input,
        ) -> Self::Output {
            self.check_inv(client, input)
        }
    }

    struct RootClient {}

    impl Default for RootClient {
        fn default() -> Self {
            Self {}
        }
    }

    impl PersistentClient for RootClient {
        fn reset(&mut self) {
            unimplemented!();
        }
    }

    const FILE_NAME: &str = "test/check_inv.pool";
    const FILE_SIZE: usize = 8 * 1024;

    /// 언제 crash나든 invariant 보장함을 보이는 테스트: flag=1 => value=42
    #[test]
    fn check_inv() {
        // 커맨드에 RUST_LOG=debug 포함시 debug! 로그 출력
        env_logger::init();

        // 풀 새로 만들기를 시도. 새로 만들기를 성공했다면 true
        let is_new_file = Pool::create::<RootObj, RootClient>(FILE_NAME, FILE_SIZE).is_ok();

        // 풀 열기
        let pool_handle = Pool::open(FILE_NAME).unwrap();
        let mut root_ptr = pool_handle.get_root::<RootObj, RootClient>().unwrap();
        let (root_obj, root_client) = unsafe { root_ptr.deref_mut() };

        // 새로 만든 풀이라면 루트 오브젝트 초기화
        if is_new_file {
            // TODO: 여기서 루트 오브젝트 초기화하기 전에 터지면 문제 발생
            // - 문제: 다시 열었을 때 루트 오브젝트를 (1) 다시 초기화해야하는지 (2) 초기화가 잘 됐는지 구분 힘듦
            // - 방안: 풀의 메타데이터 초기화할때 같이 초기화하고, 초기화가 잘 되었는지 나타내는 플래그 사용
            debug!("init root");
            *root_obj = RootObj {
                value: 0,
                flag: false,
            };
        }

        // entry point of persistent op
        root_obj.persistent_op_mut(root_client, ()).unwrap();
    }
}

#[cfg(test)]
mod test_node {
    // use crate::plocation::pool::*;
    // use env_logger as _;
    // use log as _;
    // use std::fs::remove_file;

    // struct Node {
    //     value: usize,
    //     next: PersistentPtr<Node>,
    // }

    // impl Node {
    //     fn new(value: usize) -> Self {
    //         Self {
    //             value,
    //             next: PersistentPtr::null(),
    //         }
    //     }
    // }

    // /// persistent pool에 노드를 할당하고 다시 열었을 때 매핑된 주소가 바뀌어도 할당되었던 노드를 잘 따라가는지를 확인
    // /// idempotency 테스트는 아님: persistent location이 잘 동작하는 지 확인하기 위한 테스트
    // #[test]
    // fn append_one_node() {
    //     const FILE_NAME: &str = "test/append_one_node.pool";
    //     const FILE_SIZE: usize = 8 * 1024;

    //     // 루트 오브젝트로 Node를 가진 8MB 크기의 풀 파일 새로 생성
    //     let _ = remove_file(FILE_NAME);
    //     let _ = Pool::create::<Node>(FILE_NAME, FILE_SIZE).unwrap();

    //     // 첫 번째 open: 노드 할당 후 루트 오브젝트에 연결
    //     let mapped_addr1 = {
    //         let pool_handle = Pool::open(FILE_NAME).unwrap();
    //         let mapped_addr1 = pool_handle.start();

    //         // 첫 번째 open이므로 루트 오브젝트부터 초기화
    //         let mut head = pool_handle.get_root::<Node>().unwrap();
    //         let head = unsafe { head.deref_mut() };
    //         *head = Node::new(0);

    //         // 풀에 새로운 노드 할당, 루트 오브젝트에 연결
    //         // 결과: head(val: 0) -> node1(val: 1) -> ㅗ
    //         if head.next.is_null() {
    //             let mut node1 = pool_handle.alloc::<Node>();
    //             unsafe {
    //                 *node1.deref_mut() = Node::new(1);
    //             }
    //             // TODO: 여기서 터지면 node1은 leak됨. allocator 구현 후 이러한 leak도 없게하기
    //             head.next = node1;
    //         }

    //         // NOTE
    //         // - 여기서 풀을 닫지 않아야 두 번째 open할 때 다른 주소에 매핑됨
    //         // - 풀을 닫으면 같은 파일에 대해선 같은 주소로 매핑
    //         mapped_addr1
    //     };

    //     // 두 번째 open: 첫 번째에서 구성한 풀이 다른 주소로 매핑되어도 노드를 잘 따라가는지 확인
    //     {
    //         let pool_handle = Pool::open(FILE_NAME).unwrap();
    //         let mapped_addr2 = pool_handle.start();
    //         // 첫 번째 open의 매핑 정보가 drop되기 전에 두 번째 open을 하므로, 다른 주소에 매핑됨을 보장
    //         assert_ne!(mapped_addr1, mapped_addr2);

    //         // 첫 번째 open에서 구성한 풀대로 노드를 잘 따라가는지 확인
    //         let head = pool_handle.get_root::<Node>().unwrap();
    //         let head = unsafe { head.deref() };
    //         let node1 = unsafe { head.next.deref() };
    //         // 확인하기: head(val: 0) -> node1(val: 1) -> ㅗ
    //         assert_eq!(head.value, 0);
    //         assert_eq!(node1.value, 1);
    //         assert!(node1.next.is_null());

    //         Pool::close();
    //     };
    // }

    // TODO: allocator 구현 후 테스트
    // #[test]
    // fn append_n_node() {
    //     const N: usize = 100;

    //     // 첫 번째 open: pm pool로 사용할 파일을 새로 만들고 그 안에 N개의 노드를 넣음
    //     let _ = remove_file("append_n_node.pool");
    //     {
    //         let mut head = Pool::open::<Node>("append_n_node.pool").unwrap();

    //         // N개의 노드 넣기
    //         let mut p = head.deref_mut();
    //         for i in 0..N {
    //             let mut node = PPtr::<Node>::new();
    //             node.value = i + 1; // 각 노드의 값은 자신이 몇 번째 노드인지랑 같음
    //             p.next = node;
    //             p = p.next.deref_mut();
    //         }
    //     }

    //     // 두 번째 open: 첫 번째에서 구성한 pool이 다른 주소로 매핑되어도 노드를 잘 따라가는지 확인
    //     {
    //         let head: PPtr<Node> = Pool::open::<Node>("append_n_node.pool").unwrap();

    //         // N-1번째 노드까지 따라가면서 첫 번째 open에서 구성한 대로 되어있는지 확인
    //         let mut p = head.deref();
    //         for i in 0..N {
    //             assert_eq!(p.value, i);
    //             p = p.next.deref();
    //         }
    //         // N번째 노드 확인
    //         assert_eq!(p.value, N);
    //         assert!(p.next.is_null());
    //     }
    // }
}
