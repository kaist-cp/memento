//! Persistent Pool
//!
//! # Pool Address Layout
//!
//! [ metadata | root object |            ...               ]
//! ^ base     ^ base + root offset                         ^ end
use std::fs::OpenOptions;
use std::io::Error;
use std::mem;

use super::ptr::PersistentPtr;
use super::utils::*;
use memmap::*;

/// 풀의 메타데이터 저장소 (e.g. Persistent Pointer가 참조할 때 시작주소를 사용)
static mut POOL: Option<&mut Pool> = None;
/// 메모리 매핑에 사용한 오브젝트를 담고 있을 저장소 (drop으로 인해 매핑 해제되지 않게끔 유지하는 역할)
static mut MMAP: Option<MmapMut> = None;

/// 풀의 메타데이터 저장 및 풀 함수(e.g. Pool::open, Pool::alloc, ..) 호출을 위한 역할
#[derive(Debug)]
pub struct Pool {
    start: usize,
    end: usize,
    root_offset: usize,
    // TODO: 풀을 위한 메타데이터는 여기에 필드로 추가
}

impl Pool {
    fn init(&mut self, start: usize, end: usize) {
        self.start = start;
        self.end = end;
        self.root_offset = mem::size_of::<Pool>();
    }

    /// 풀 생성
    pub fn create<T>(filepath: &str, size: u64) -> Result<(), Error> {
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
        file.set_len(size).unwrap();

        // 2. 파일을 풀 레이아웃에 맞게 세팅
        let mut mmap = unsafe { memmap::MmapOptions::new().map_mut(&file).unwrap() };
        // 메타데이터 초기화
        let start = mmap.get_mut(0).unwrap() as *const _ as usize;
        let end = start + size as usize + 1;
        let pool = unsafe { read_addr::<Pool>(start) };
        pool.init(start, end);

        // TODO: 루트 오브젝트 초기화
        // let addr_root = unsafe { read_addr::<T>(start + pool.root_offset) };
        // *addr_root = T::default();
        Ok(())
    }

    /// 풀 열기: 파일을 persistent heap으로 매핑 후 루트 오브젝트를 가리키는 포인터 반환
    ///
    /// # Examples
    ///
    /// ```
    /// use compositional_persistent_object::plocation::pool::Pool;
    ///
    /// let mut head = Pool::open::<i32>("foo.pool").unwrap();
    /// *head = 5;
    /// assert_eq!(*head, 5);
    /// ```
    pub fn open<T>(filepath: &str) -> Result<PersistentPtr<T>, Error> {
        // 1. 파일 열기
        let file = match OpenOptions::new().read(true).write(true).open(filepath) {
            Ok(file) => file,
            // 파일이 존재하지 않는다면 실패
            Err(e) => return Err(e),
        };

        // 2. 파일을 가상주소에 매핑
        let mut mmap = unsafe { memmap::MmapOptions::new().map_mut(&file).unwrap() };
        // 풀의 시작, 끝 주소 세팅
        let base = mmap.get_mut(0).unwrap() as *const _ as usize;
        // 풀의 메타데이터를 담는 inner 읽기 (inner는 풀의 시작주소에 위치)
        let pool = unsafe { read_addr::<Pool>(base) };

        // 3. 풀의 루트 오브젝트를 가리키는 포인터 반환
        let root_obj = PersistentPtr::from(pool.root_offset);
        unsafe {
            POOL = Some(pool);
            MMAP = Some(mmap); // drop으로 인해 매핑 해제되지 않게끔 유지
        }
        Ok(root_obj)
    }

    /// 풀 닫기
    pub fn close() {
        unsafe {
            POOL = None;
            MMAP = None; // 매핑된 것이 MMAP에 저장되어있었다면 이 때 매핑 해제됨
        }
    }

    /// 풀 열려있는지 확인
    pub fn is_open() -> bool {
        unsafe { POOL.is_some() }
    }

    /// 풀의 시작주소 반환
    pub fn start() -> usize {
        if !Pool::is_open() {
            panic!("No memory pool is open.");
        }
        unsafe { POOL.as_deref().unwrap().start }
    }

    /// 풀의 끝주소 반환
    pub fn end() -> usize {
        if !Pool::is_open() {
            panic!("No memory pool is open.");
        }
        unsafe { POOL.as_deref().unwrap().end }
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
        let addr = pptr.get_addr();
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

        // 첫 번째 open: persistent pool로 사용할 파일을 새로 만들고 그 안에 1개의 노드를 넣음
        {
            let mut head = Pool::open::<Node>("append_one_node.pool").unwrap();
            *head = Node::new(0);

            // 풀에 새로운 노드 할당, 루트 오브젝트에 연결
            // 결과: head node(root obj) -> node1 -> ㅗ
            let mut node1 = Pool::alloc::<Node>();
            *node1 = Node::new(1);
            head.next = node1;
            Pool::close();
        };

        // 두 번째 open: 첫 번째에서 구성한 풀이 다른 주소로 매핑되어도 노드를 잘 따라가는지 확인
        {
            let head = Pool::open::<Node>("append_one_node.pool").unwrap();
            assert_eq!(head.value, 0);
            assert_eq!(head.next.value, 1);
            assert!(head.next.next.is_null());
            Pool::close();
        };
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
