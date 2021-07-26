//! Persistent Pool
//!
//! # Pool Address Layout
//!
//! [ metadata | root object |            ...               ]
//! ^ base     ^ base + root offset                         ^ end

// TODO(allocator 구현(#50))
// - 현재 pool은 임시로 고정주소를 allocation하게끔 되어 있음

// TODO(pool 여러 개 지원(#48))
// - 현재는 하나의 pool만 열 수 있음
// - 동시에 여러 개의 pool을 열 수 있도록 지원

use std::fs::OpenOptions;
use std::io::Error;
use std::mem;

use super::ptr::PersistentPtr;
use memmap::*;

/// 풀의 런타임 정보 (DRAM에 저장)
/// - e.g. Persistent Pointer가 참조할 때 풀의 시작주소를 사용
static mut POOL_RUNTIME_INFO: PoolRuntimeInfo = PoolRuntimeInfo {
    mmap: None,
    start: 0,
    len: 0,
};

// TODO: 풀의 메타데이터 (PM에 저장)
// static mut POOL: Pool = Pool { root_offset: 0, ... };
// - 현재는 풀의 메타데이터가 root_offset 뿐이기 때문에 런타임 정보처럼 계속 유지할 필요없음
// - 향후 client 구현시엔 계속 유지하며 이용해야함

/// 풀의 런타임 정보를 담는 역할
#[derive(Debug)]
pub struct PoolRuntimeInfo {
    mmap: Option<MmapMut>, // 메모리 매핑에 사용한 오브젝트 저장 (drop으로 인해 매핑 해제되지 않게끔 유지하는 역할)
    start: usize,
    len: usize,
}

impl PoolRuntimeInfo {
    fn new(mmap: MmapMut, start: usize, len: usize) -> Self {
        Self {
            mmap: Some(mmap),
            start,
            len,
        }
    }
}

/// 풀의 메타데이터 저장 및 풀 함수(e.g. Pool::open, Pool::alloc, ..) 호출을 위한 역할
#[derive(Debug)]
pub struct Pool {
    root_offset: usize,
    // TODO: 풀을 위한 메타데이터는 여기에 필드로 추가
}

impl Pool {
    /// 메타데이터 초기화
    fn init(&mut self) {
        self.root_offset = mem::size_of::<Pool>();
    }

    /// 풀 생성
    pub fn create<T>(filepath: &str, size: usize) -> Result<(), Error> {
        // 1. 파일 생성 및 크기 세팅
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(filepath)
        {
            Ok(file) => file,
            // 파일이 이미 존재하면 실패
            Err(e) => return Err(e),
        };
        file.set_len(size as u64).unwrap();

        // 2. 파일을 풀 레이아웃에 맞게 세팅
        let mut mmap = unsafe { memmap::MmapOptions::new().map_mut(&file).unwrap() };
        let start = mmap.get_mut(0).unwrap() as *const _ as usize;
        let pool = unsafe { &mut *(start as *mut Pool) };
        // 메타데이터 초기화
        pool.init();
        // TODO: 루트 오브젝트 초기화를 유저가 하는 게 아니라 여기서 하기?
        Ok(())
    }

    /// 풀 열기: 파일을 persistent heap으로 매핑 후 루트 오브젝트를 가리키는 포인터 반환
    ///
    /// # Examples
    ///
    /// ```
    /// use std::fs::remove_file;
    /// use compositional_persistent_object::plocation::pool::Pool;
    ///
    /// // 기존의 파일은 제거하고 새로 생성
    /// let _ = remove_file("foo.pool");
    /// let _ = Pool::create::<i32>("foo.pool", 8 * 1024);
    ///
    /// // 풀 열기
    /// let mut head = Pool::open::<i32>("foo.pool").unwrap();
    /// unsafe {
    ///     *head.deref_mut() = 5;
    ///     assert_eq!(*head.deref(), 5);
    /// }
    /// ```
    pub fn open<T>(filepath: &str) -> Result<PersistentPtr<T>, Error> {
        // 1. 파일 열기
        let file = match OpenOptions::new().read(true).write(true).open(filepath) {
            Ok(file) => file,
            // 파일이 존재하지 않는다면 실패
            Err(e) => return Err(e),
        };

        // 2. 메모리 매핑 후 런타임 정보(e.g. 시작 주소) 세팅
        let mut mmap = unsafe { memmap::MmapOptions::new().map_mut(&file).unwrap() };
        let start = mmap.get_mut(0).unwrap() as *const _ as usize;
        unsafe {
            POOL_RUNTIME_INFO =
                PoolRuntimeInfo::new(mmap, start, file.metadata().unwrap().len() as usize);
        }

        // 3. 루트 오브젝트를 가리키는 포인터 반환
        let pool = unsafe { &*(start as *const Pool) };
        let root_obj = PersistentPtr::from(pool.root_offset);
        Ok(root_obj)
    }

    /// 풀 닫기
    pub fn close() {
        unsafe {
            // 매핑된 것이 POOL.mmap에 저장되어있었다면 이 때 매핑 해제됨
            POOL_RUNTIME_INFO.mmap = None;
        }
    }

    /// 풀 열려있는지 확인
    pub fn is_open() -> bool {
        unsafe { POOL_RUNTIME_INFO.mmap.is_some() }
    }

    /// 풀의 시작주소 반환
    pub fn start() -> usize {
        if !Pool::is_open() {
            panic!("No memory pool is open.");
        }
        unsafe { POOL_RUNTIME_INFO.start }
    }

    /// 풀의 끝주소 반환
    pub fn end() -> usize {
        if !Pool::is_open() {
            panic!("No memory pool is open.");
        }
        unsafe { POOL_RUNTIME_INFO.start + POOL_RUNTIME_INFO.len }
    }

    /// 풀에 T의 크기만큼 할당 후 이를 가리키는 포인터 얻음
    pub fn alloc<T>() -> PersistentPtr<T> {
        if !Pool::is_open() {
            panic!("No memory pool is open.");
        }

        // TODO: 실제 allocator 사용 (현재는 base + 1024 위치에 할당된 것처럼 동작)
        // let addr_allocated = allocator.alloc(mem::size_of::<T>());
        let addr_allocated = 1024;
        PersistentPtr::from(addr_allocated)
    }

    /// persistent pointer가 가리키는 풀 내부의 메모리 블록 할당해제
    pub fn free<T>(_pptr: &mut PersistentPtr<T>) {
        if !Pool::is_open() {
            panic!("No memory pool is open.");
        }

        // TODO
    }

    /// persistent pointer가 풀에 속하는지 확인
    fn _valid<T>(pptr: PersistentPtr<T>) -> bool {
        let addr = pptr.to_transient_ptr() as usize;
        addr >= Pool::start() && addr < Pool::end()
    }
}
#[cfg(test)]
mod test {
    use crate::plocation::pool::*;
    use std::fs::remove_file;

    struct Node {
        value: usize,
        next: PersistentPtr<Node>,
    }

    impl Node {
        fn new(value: usize) -> Self {
            Self {
                value,
                next: PersistentPtr::null(),
            }
        }
    }

    /// persistent pool에 노드를 할당하고, 다시 열었을 때 매핑된 주소가 바뀌어도 잘 따라가는지 테스트
    #[test]
    fn append_one_node() {
        // 기존의 파일은 삭제하고 루트 오브젝트로 Node를 가진 8MB 크기의 풀 파일 새로 생성
        let _ = remove_file("append_one_node.pool");
        let _ = Pool::create::<Node>("append_one_node.pool", 8 * 1024).unwrap();

        // 첫 번째 open: 루트 오브젝트 초기화하고 노드 1개를 할당해서 연결함
        let mapped_addr1 = {
            let mut head = Pool::open::<Node>("append_one_node.pool").unwrap();
            let mapped_addr1 = Pool::start();
            // 루트 오브젝트 초기화
            unsafe {
                *head.deref_mut() = Node::new(0);
            }

            // 풀에 새로운 노드 할당, 루트 오브젝트에 연결
            // 결과: head(val: 0) -> node1(val: 1) -> ㅗ
            let mut node1 = Pool::alloc::<Node>();
            unsafe {
                *node1.deref_mut() = Node::new(1);
                head.deref_mut().next = node1;
            }

            // NOTE
            // - 여기서 풀을 닫지 않아야 두 번째 open할 때 다른 주소에 매핑됨
            // - 풀을 닫으면 같은 파일에 대해선 같은 주소로 매핑
            mapped_addr1
        };

        // 두 번째 open: 첫 번째에서 구성한 풀이 다른 주소로 매핑되어도 노드를 잘 따라가는지 확인
        let mapped_addr2 = {
            let head = Pool::open::<Node>("append_one_node.pool").unwrap();
            let mapped_addr2 = Pool::start();

            unsafe {
                assert_eq!(head.deref().value, 0);
                assert_eq!(head.deref().next.deref().value, 1);
                assert!(head.deref().next.deref().next.is_null());
            }

            Pool::close();
            mapped_addr2
        };

        // 다른 주소에 매핑되었어야 이 테스트의 의미가 있음
        assert_ne!(mapped_addr1, mapped_addr2);
    }

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
