//! Persistent Pool
//!
//! # Pool Address Layout
//!
//! [ metadata | root object |            ...               ]
//! ^ base     ^ base + root offset                         ^ endbase + size of file
use crate::plocation::utils::*;
use std::fs::OpenOptions;
use std::mem;
use std::path::Path;

use super::ptr::PPtr;
use memmap::*;

pub static mut POOL_START: usize = 0; // 풀의 시작 주소
static mut POOL_END: usize = 0; // 풀의 끝 주소
static mut MMAP: Option<MmapMut> = None;

/// 풀의 메타데이터를 담을 구조체
struct PoolInner {
    root_offset: usize,
    // TODO: 필요한 메타데이터가 생기면 여기에 필드로 추가
}

impl PoolInner {
    fn init(&mut self) {
        self.root_offset = mem::size_of::<PoolInner>();
    }
}

/// 풀 함수(e.g. Pool::open, Pool::alloc, ..) 호출을 위한 더미 역할
#[derive(Debug)]
pub struct Pool {}

impl Pool {
    /// 파일을 persistent heap으로 매핑 후 루트 오브젝트를 가리키는 포인터 반환
    ///
    /// # Examples
    ///
    /// ```
    /// use smjeon_test::pheap::pool::Pool;
    ///
    /// let mut head = Pool::open::<i32>("foo.pool").unwrap();
    /// *head = 5;
    /// assert_eq!(*head, 5);
    /// ```
    pub fn open<T: Default>(filepath: &str) -> Result<PPtr<T>, String> {
        // 1. 파일 열기
        let is_new_file = !Path::new(filepath).exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filepath)
            .unwrap();
        // 새로 만든 파일이라면 8MB로 세팅
        if is_new_file {
            file.set_len(8 * 1024).unwrap();
        }

        // 2. 파일을 가상주소에 매핑
        let mut mmap = unsafe { memmap::MmapOptions::new().map_mut(&file).unwrap() };
        // 풀의 시작, 끝 주소 세팅
        let size = file.metadata().unwrap().len() as usize;
        let base = mmap.get_mut(0).unwrap() as *const _ as usize;
        unsafe {
            POOL_START = base;
            POOL_END = POOL_START + size + 1;
            MMAP = Some(mmap); // drop으로 인해 매핑 해제되지 않게끔 유지
        }
        // 풀의 메타데이터를 담는 inner 읽기 (inner는 풀의 시작주소에 위치)
        let inner = unsafe { read_addr::<PoolInner>(POOL_START) };
        if is_new_file {
            // 새로 만든 파일이라면 풀의 메타데이터 초기화
            inner.init();
        }

        // 3. 풀의 루트 오브젝트를 가리키는 포인터 반환
        let mut root: PPtr<T> = PPtr::from_off(inner.root_offset);
        if is_new_file {
            // 새로 만든 파일이라면 루트 오브젝트 초기화
            *root = T::default(); // TODO: root.init() 형태로 바꾸기?
        }
        Ok(root)
    }

    pub fn close() {
        unsafe {
            POOL_START = 0;
            MMAP = None; // 매핑된 것이 MMAP에 저장되어있었다면 이 때 매핑 해제됨
        }
    }

    /// 풀에 size를 담을 수 있는 메모리 블록 할당 후, 할당된 주소의 offset 반환
    pub fn alloc(_size: usize) -> usize {
        // TODO: 실제 allocator 사용 (현재는 base + 1024 위치에 할당된 것처럼 동작)
        1024
    }

    /// persistent pointer가 가리키는 메모리 블록 할당해제
    pub fn free<T: Default>(_pptr: &mut PPtr<T>) {
        // TODO
    }

    /// persistent pointer가 풀에 속하는지 확인
    fn _valid<T: Default>(pptr: PPtr<T>) -> bool {
        let addr = pptr.get_addr();
        unsafe { addr >= POOL_START && addr < POOL_END }
    }
}
#[cfg(test)]
mod test {
    use crate::plocation::pool::*;
    use std::fs::remove_file;

    struct Node {
        value: usize,
        next: PPtr<Node>,
    }

    impl Default for Node {
        fn default() -> Self {
            Self {
                value: 0,
                next: PPtr::null(),
            }
        }
    }

    #[test]
    fn append_one_node() {
        // 기존의 파일은 삭제
        let _ = remove_file("append_one_node.pool");

        // 첫 번째 open: persistent pool로 사용할 파일을 새로 만들고 그 안에 1개의 노드를 넣음
        {
            // default() 함수로 초기화됨
            let mut head = Pool::open::<Node>("append_one_node.pool").unwrap();
            assert_eq!(head.value, 0);
            assert!(head.next.is_null());

            // 풀안에 새로운 노드 할당, 루트 오브젝트에 연결
            // 결과: head node(root obj) -> node1 -> ㅗ
            let mut node1 = PPtr::<Node>::new();
            node1.value = 1;
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
