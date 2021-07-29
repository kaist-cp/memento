//! Persistent Memory Pool
//!
//! 파일을 persistent heap으로서 가상주소에 매핑하고, 그 메모리 영역을 관리하는 메모리 "풀"
//!
//! # Pool Address Layout
//!
//! [ metadata | root object |            ...               ]
//! ^ base     ^ base + root offset                         ^ end

use std::fs::OpenOptions;
use std::io::Error;
use std::mem;

use super::global;
use super::ptr::PersistentPtr;
use memmap::*;

/// 풀의 런타임정보를 담는 역할
#[derive(Debug)]
struct PoolRuntimeData {
    /// 메모리 매핑에 사용한 오브젝트 (drop으로 인해 매핑 해제되지 않게끔 들고 있어야함)
    mmap: MmapMut,

    /// 풀의 시작 주소
    start: usize,

    /// 풀의 길이
    len: usize,
    // TODO: 영속될 필요없는 데이터는 여기 넣어서 성능이득을 얻을 수 있나 고려해보기
}

/// 풀의 메타데이터를 담는 역할
#[derive(Debug)]
struct PoolMetadata {
    /// 풀의 시작주소로부터 루트 오브젝트까지의 거리
    root_offset: usize,
    // TODO: 풀의 메타데이터는 여기에 필드로 추가
}

impl PoolMetadata {
    /// 메타데이터 초기화
    fn init(&mut self) {
        // e.g. 메타데이터 크기(size_of::<Pool>)가 16이라면, 루트 오브젝트는 풀의 시작주소+16에 위치
        self.root_offset = mem::size_of::<Pool>();
    }
}

/// 풀 함수(e.g. Pool::open, Pool::alloc, ..) 호출 및 풀의 정보를 가지고 있을 역할
///
/// 데이터 접근성능을 위해 DRAM 혹은 PM에서 유지할 정보를 필드로 구분
/// - DRAM에서 유지할 풀의 런타임정보
/// - PM에서 유지할 풀의 메타데이터
#[derive(Debug)]
pub struct Pool {
    /// 풀의 런타임정보
    /// - 이 필드에 접근하는 것은 DRAM 접근을 의미
    /// - e.g. Persistent Pointer가 참조할 때, 이 정보에 담긴 풀의 시작주소를 base로 사용
    runtime_data: PoolRuntimeData,

    /// 풀의 메타데이터
    /// - 이 필드에 접근하는 것은 PM 접근을 의미
    /// - e.g. 실행중에 allocator를 위한 메타데이터를 읽으며/업데이트 해줘야할 거라 추정
    metadata: PoolMetadata,
}

impl Pool {
    /// 풀 생성: 풀로서 사용할 파일을 생성하고 풀 레이아웃에 맞게 파일의 내부구조 초기화
    pub fn create<T>(filepath: &str, size: usize) -> Result<(), Error> {
        // 1. 파일 생성 및 크기 세팅 (파일이 이미 존재하면 실패)
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(filepath)?;
        file.set_len(size as u64)?;

        // 2. 파일을 풀 레이아웃에 맞게 초기화
        let mut mmap = unsafe { memmap::MmapOptions::new().map_mut(&file)? };
        let start = mmap.get_mut(0).unwrap() as *const _ as usize;
        let pool_metadata = unsafe { &mut *(start as *mut PoolMetadata) };
        // 메타데이터 초기화
        pool_metadata.init();

        // TODO: 루트 오브젝트의 필드 초기화를 여기서 할지 고민 필요
        // - 현재는 유저가 해야함
        // - 여기서 풀 생성시에 해준다면, T에 default trait를 강제해야함
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
        // 1. 파일 열기 (파일이 존재하지 않는다면 실패)
        let file = OpenOptions::new().read(true).write(true).open(filepath)?;

        // 2. 메모리 매핑 후 글로벌 풀 세팅
        let mut mmap = unsafe { memmap::MmapOptions::new().map_mut(&file)? };
        let start = mmap.get_mut(0).unwrap() as *const _ as usize;
        global::init(Pool {
            runtime_data: PoolRuntimeData {
                mmap,
                start,
                len: file.metadata()?.len() as usize,
            },
            metadata: unsafe { std::ptr::read(start as *const PoolMetadata) },
        });

        // 3. 루트 오브젝트를 가리키는 포인터 반환
        let root_obj = PersistentPtr::from(global::global_pool().unwrap().metadata.root_offset);
        Ok(root_obj)
    }

    /// 풀 닫기
    pub fn close() {
        // 메모리 매핑에 사용한 `MmapMut` 오브젝트가 글로벌 풀 내부의 `mmap` 필드에 저장되어있었다면 이때 매핑 해제됨
        global::clear();
    }

    /// 풀의 시작주소 반환
    pub fn start(&self) -> usize {
        self.runtime_data.start
    }

    /// 풀의 끝주소 반환
    pub fn end(&self) -> usize {
        self.runtime_data.start + self.runtime_data.len
    }

    /// 풀에 T의 크기만큼 할당 후 이를 가리키는 포인터 얻음
    pub fn alloc<T>(&self) -> PersistentPtr<T> {
        // TODO: 실제 allocator 사용 (현재는 base + 1024 위치에 할당된 것처럼 동작)
        // let addr_allocated = self.allocator.alloc(mem::size_of::<T>());
        let addr_allocated = 1024;
        PersistentPtr::from(addr_allocated)
    }

    /// persistent pointer가 가리키는 풀 내부의 메모리 블록 할당해제
    pub fn free<T>(&self, _pptr: &mut PersistentPtr<T>) {
        todo!("pptr이 가리키는 메모리 블록 할당해제")
    }
}
#[cfg(test)]
mod test {
    use super::global::global_pool;
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
            let mapped_addr1 = global_pool().unwrap().start();
            // 루트 오브젝트 초기화
            unsafe {
                *head.deref_mut() = Node::new(0);
            }

            // 풀에 새로운 노드 할당, 루트 오브젝트에 연결
            // 결과: head(val: 0) -> node1(val: 1) -> ㅗ
            let mut node1 = global_pool().unwrap().alloc::<Node>();
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
            let mapped_addr2 = global_pool().unwrap().start();

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
