//! ssmem allocator

use libc::c_void;
use std::{
    alloc::Layout,
    cell::RefCell,
    intrinsics,
    mem::size_of,
    ptr::{null, null_mut},
};

use crate::pmem::{clflush, prefetchw, PoolHandle};

/* ****************************************************************************************
 */
/* parameters */
/* ****************************************************************************************
 */

const SSMEM_TRANSPARENT_HUGE_PAGES: usize = 0; /* Use or not Linux transparent huge pages */
const SSMEM_ZERO_MEMORY: usize = 1; /* Initialize allocated memory to 0 or not */
const SSMEM_GC_FREE_SET_SIZE: usize = 507; /* mem objects to free before doing a GC pass */
const SSMEM_GC_RLSE_SET_SIZE: usize = 3; /* num of released object before doing a GC pass */

/// memory-chunk size that each threads gives to the allocators
pub const SSMEM_DEFAULT_MEM_SIZE: usize = 32 * 1024 * 1024;

const SSMEM_MEM_SIZE_DOUBLE: usize = 0; /* if the allocator is out of memory, should it allocate \
                                         a 2x larger chunk than before? (in order to stop asking \
                                        for memory again and again */
const SSMEM_MEM_SIZE_MAX: usize = 4 * 1024 * 1024 * 1024; /* absolute max chunk size \
                                                          (e.g., if doubling is 1) */

/* increase the thread-local timestamp of activity on each ssmem_alloc() and/or
ssmem_free() call. If enabled (>0), after some memory is alloced and/or
freed, the thread should not access ANY ssmem-protected memory that was read
(the reference were taken) before the current alloc or free invocation. If
disabled (0), the program should employ manual SSMEM_SAFE_TO_RECLAIM() calls
to indicate when the thread does not hold any ssmem-allocated memory
references. */

// @seungminjeon: SSMEM_TS_INCR_ON은, timestamp를 언제 올리냐를 선택
const SSMEM_TS_INCR_ON_NONE: usize = 0; // 유저가 직접 올려야함. 아마 ssmem_ts_next() 이용해야할듯
const SSMEM_TS_INCR_ON_BOTH: usize = 1; // alloc or free할 때 올림
const SSMEM_TS_INCR_ON_ALLOC: usize = 2; // alloc할 때 올림
const SSMEM_TS_INCR_ON_FREE: usize = 3; // free할 때 올림

const SSMEM_TS_INCR_ON: usize = SSMEM_TS_INCR_ON_FREE;

/* ****************************************************************************************
 */
/* help definitions */
/* ****************************************************************************************
 */
// #define ALIGNED(N) __attribute__((aligned(N))) // TODO: 필요?
const CACHE_LINE_SIZE: usize = 64;

/* ****************************************************************************************
 */
/* data structures used by ssmem */
/* ****************************************************************************************
 */

/// an ssmem allocator
#[derive(Debug)]
#[repr(align(64))]
pub struct ssmem_allocator {
    /// allocator가 사용중인 메모리 (사용중인 memory chunk의 시작주소를 가리킴)
    mem: *mut c_void,

    /// pointer to the next addrr to be allocated (사용중인 memory chunk가 어디까지 사용되었는지를 가리킴. 다음 할당 요청시 이 주소를 반환)
    mem_curr: usize,

    /// memory chunk 하나의 크기
    mem_size: usize,

    /// allocator가 사용중인 메모리의 총 크기 (e.g. memory chunk 2개 가지고 있으면 mem_size * 2)
    tot_size: usize,

    /// free set 하나에 저장될 수 있는 obj의 수
    fs_size: usize,

    /// 사용중인 memory chunk들의 리스트
    pub mem_chunks: *const ssmem_list,

    /// TODO doc
    ts: *mut ssmem_ts,

    /// TODO: doc (free 되어 collect되길 기다리고 있는 obj들을 모아둠. 한 set에 obj를 507개까지 담아둘 수 있음)
    free_set_list: *mut ssmem_free_set,

    /// free_set_list에 있는 free set의 수
    free_set_num: usize,

    /// TODO: doc (free 후 collect 까지되어서 재사용가능한 obj들을 모아둠. alloc시 여기 있는 거부터 빼감)
    collected_set_list: *mut ssmem_free_set,

    /// collected_set_list에 있는 free set의 수
    collected_set_num: usize,

    /// TODO: doc
    available_set_list: *mut ssmem_free_set,

    /// TODO: doc
    released_num: usize,

    /// TODO: doc
    released_mem_list: *const ssmem_released,
    // TODO: cache line*2 크기로 패딩?
}

#[repr(align(64))]
struct ssmem_ts {
    version: usize,
    id: usize,
    next: *mut ssmem_ts,
    // TODO: cache line 크기로 패딩?
}

#[repr(align(64))]
struct ssmem_free_set {
    ts_set: *mut usize,
    size: usize,
    curr: usize,
    set_next: *mut ssmem_free_set,

    /// 이 주소부터 free obj들이 위치함
    set: *mut usize,
}

// TODO: 안쓰이는 struct임. 지워도 될듯?
struct ssmem_released {
    ts_set: *const usize,
    mem: *const c_void,
    next: *const ssmem_released,
}

/// TODO: doc
#[derive(Debug)]
pub struct ssmem_list {
    /// pointing memory chunk
    pub obj: *const c_void,

    /// pointing next memory chunk
    pub next: *const ssmem_list,
}

/* ****************************************************************************************
 */
/* ssmem interface */
/* ****************************************************************************************
 */

/// initialize an allocator with the default number of objects
// TODO: therad-local recovery시 이 alloc_init은 다시 호출하면 안될 듯.
//  - 복구를 위해선 allocator의 필드 mem_chunk 값을 보존하고 있어야하는데, 이 함수를 호출하면 mem_chunk를 재설정함
pub fn ssmem_alloc_init(
    a: *mut ssmem_allocator,
    size: usize,
    id: isize,
    pool: Option<&PoolHandle>,
) {
    ssmem_alloc_init_fs_size(a, size, SSMEM_GC_FREE_SET_SIZE, id, pool)
}

/// initialize an allocator and give the number of objects in free_sets
pub fn ssmem_alloc_init_fs_size(
    a: *mut ssmem_allocator,
    size: usize,
    free_set_size: usize,
    id: isize,
    pool: Option<&PoolHandle>,
) {
    ssmem_num_allocators.with(|x| *x.borrow_mut() += 1);
    ssmem_allocator_list.with(|x| {
        let next = *x.borrow();
        *x.borrow_mut() = ssmem_list_node_new(a as *mut _, next, pool)
    });

    // 첫 memory chunk 할당
    let a = unsafe { a.as_mut() }.unwrap();
    a.mem = alloc(
        Layout::from_size_align(size, CACHE_LINE_SIZE).unwrap(),
        pool,
    );
    assert!(a.mem != null_mut());

    a.mem_curr = 0;
    a.mem_size = size;
    a.tot_size = size;
    a.fs_size = free_set_size;

    // memory chunk를 zero-initalize
    ssmem_zero_memory(a);

    let new_mem_chunks: *const ssmem_list = ssmem_list_node_new(a.mem, null(), pool);
    barrier(new_mem_chunks);

    a.mem_chunks = new_mem_chunks;
    barrier(a.mem_chunks);
    ssmem_gc_thread_init(a, id, pool);

    a.free_set_list = ssmem_free_set_new(a.fs_size, null_mut(), pool);
    a.free_set_num = 1;

    a.collected_set_list = null_mut();
    a.collected_set_num = 0;

    a.available_set_list = null_mut();

    a.released_mem_list = null();
    a.released_num = 0;
}

/// explicitely subscribe to the list of threads in order to used timestamps for GC
// TODO: thread-local recovery시 이 gc_init 다시 호출해야할 듯.
// - 그래야 다른 스레드와 epoch 조율을 위한 `ssmem_ts`가 thread-local `ssmem_ts_local`에 세팅되고, global list `ssmem_ts_list`에 추가됨
// - 지금 로직은 재호출 시 crash 이전에 사용하던 `ssmem_ts`가 global list에 남아있음. 이게 계속 쌓이면서 메모리가 터질 순 있지만, correctness엔 문제없음
pub fn ssmem_gc_thread_init(a: *mut ssmem_allocator, id: isize, pool: Option<&PoolHandle>) {
    let a_ref = unsafe { a.as_mut() }.unwrap();
    a_ref.ts = ssmem_ts_local.with(|ts| *ts.borrow());
    if a_ref.ts.is_null() {
        a_ref.ts = alloc(
            Layout::from_size_align(size_of::<ssmem_ts>(), CACHE_LINE_SIZE).unwrap(),
            pool,
        );
        assert!(!a_ref.ts.is_null());
        ssmem_ts_local.with(|ts| {
            let prev = ts.replace(a_ref.ts);
            assert!(prev.is_null())
        });

        let ts_ref = unsafe { a_ref.ts.as_mut() }.unwrap();
        ts_ref.id = id as usize;
        ts_ref.version = 0;

        loop {
            ts_ref.next = unsafe { ssmem_ts_list };

            // TODO: c++의 `__sync_val_compare_and_swap` 대신 이걸 써도 correct한가?
            let (val, ok) = unsafe {
                intrinsics::atomic_cxchg(
                    &mut ssmem_ts_list as *mut _,
                    ts_ref.next,
                    ts_ref as *mut _,
                )
            };
            if ok {
                break;
            }
        }
        let _ = unsafe { intrinsics::atomic_xadd(&mut ssmem_ts_list_len as *mut usize, 1) };
    }
}

/// terminate the system (all allocators) and free all memory
pub fn ssmem_term(pool: Option<&PoolHandle>) {
    todo!("필요하면 구현. 기존 repo의 SOFT hash 구현에는 안쓰임")
}

/// terminate the allocator a and free all its memory.
///
/// # Safety
///
/// This function should NOT be used if the memory allocated by this allocator
/// might have been freed (and is still in use) by other allocators
pub unsafe fn ssmem_alloc_term(a: &ssmem_allocator, pool: Option<&PoolHandle>) {
    todo!("필요하면 구현. 기존 repo의 SOFT hash 구현에는 안쓰임")
}

/// allocate some memory using allocator a
pub fn ssmem_alloc(a: *mut ssmem_allocator, size: usize, pool: Option<&PoolHandle>) -> *mut c_void {
    let mut m: *mut c_void = null_mut();
    let a_ref = unsafe { a.as_mut() }.unwrap();

    /* 1st try to use from the collected memory */
    let cs = a_ref.collected_set_list;
    // free 이후 collect까지 되어 재사용 가능한 obj가 있으면 재사용
    if !cs.is_null() {
        let cs_ref = unsafe { cs.as_mut() }.unwrap();
        // fs.set[fs.curr-1]에 저장되어있는 collect된 obj 주소를 가져옴 (TODO: rust에선 이렇게 하는 게 맞나 확인)
        cs_ref.curr -= 1;
        m = unsafe { *(cs_ref.set.offset(cs_ref.curr as isize)) as *mut _ };
        prefetchw(m);

        // collected set에 남은 재사용가능 obj가 없으면,
        // 이를 free set으로 재사용 할 수 있게 available list에 넣음
        if cs_ref.curr <= 0 {
            a_ref.collected_set_list = cs_ref.set_next;
            a_ref.collected_set_num -= 1;

            ssmem_free_set_make_avail(a, cs);
        }
    }
    // 재사용가능한 obj가 없으면(i.e. collected list가 비어있으면) 블록을 새로 할당
    else {
        // 현재 사용중인 memmory chunk에 남은 공간 부족하면, 새로운 memory chunk 등록 후 사용
        if (a_ref.mem_curr + size) >= a_ref.mem_size {
            if SSMEM_MEM_SIZE_DOUBLE == 1 {
                a_ref.mem_size <<= 1;
                if a_ref.mem_size > SSMEM_MEM_SIZE_MAX {
                    a_ref.mem_size = SSMEM_MEM_SIZE_MAX;
                }
            }

            // 요청한 size가 한 memory chunk보다 크다면, 요청 크기를 담을 수 있을 만큼 memory chunk 크기를 늘림
            if size > a_ref.mem_size {
                while a_ref.mem_size < size {
                    // memory chunk 최대치 넘으면 에러
                    if a_ref.mem_size > SSMEM_MEM_SIZE_MAX {
                        eprintln!(
                            "[ALLOC] asking for memory chunk larger than max ({} MB)",
                            SSMEM_MEM_SIZE_MAX / (1024 * 1024)
                        );
                        assert!(a_ref.mem_size <= SSMEM_MEM_SIZE_MAX);
                    }

                    // memorch chunk 크기를 2배 키움
                    a_ref.mem_size <<= 1;
                }
            }

            // 새로운 memory chunk 할당
            a_ref.mem = alloc(
                Layout::from_size_align(a_ref.mem_size, CACHE_LINE_SIZE).unwrap(),
                pool,
            );
            assert!(a_ref.mem != null_mut());

            a_ref.mem_curr = 0;
            a_ref.tot_size += a_ref.mem_size;

            // allocator가 현재 쥐고 있는 memory chunk를 zero-initialize
            ssmem_zero_memory(a);

            // 새로 할당한 memory chunk를 memory chunk list에 추가
            let new_mem_chunks = ssmem_list_node_new(a_ref.mem, a_ref.mem_chunks, pool);
            barrier(new_mem_chunks);

            a_ref.mem_chunks = new_mem_chunks;
            barrier(a_ref.mem_chunks);
        }

        // 사용가능한 블록의 위치 계산 (start + offset)
        m = (a_ref.mem as usize + a_ref.mem_curr) as *mut c_void;
        a_ref.mem_curr += size;
    }

    // timestamp 증가 전략이 "alloc할 때"가 포함이면 timestamp 증가
    if SSMEM_TS_INCR_ON == SSMEM_TS_INCR_ON_ALLOC || SSMEM_TS_INCR_ON == SSMEM_TS_INCR_ON_BOTH {
        ssmem_ts_next();
    }
    m
}

/// free some memory using allocator a
pub fn ssmem_free(a: *mut ssmem_allocator, obj: *mut c_void, pool: Option<&PoolHandle>) {
    let a = unsafe { a.as_mut() }.unwrap();
    let mut fs = unsafe { a.free_set_list.as_mut() }.unwrap();

    // 현재 쥐고 있는 free set(free set list의 head)이 꽉찼으면 (1) collect 한번 돌리고 (2) 새로운 free set을 추가
    // 이때 추가되는 새로운 free set은 collect에 의해 재사용되는 free set일 수도 있고, 아예 새로 만들어진 free set일 수도 있음
    if fs.curr == fs.size {
        fs.ts_set = ssmem_ts_set_collect(fs.ts_set, pool);
        let _ = ssmem_mem_reclaim(a as *mut _, pool);

        let fs_new = ssmem_free_set_get_avail(a as *mut _, a.fs_size, a.free_set_list, pool);
        a.free_set_list = fs_new;
        a.free_set_num += 1;
        fs = unsafe { fs_new.as_mut() }.unwrap();
    }

    // fs.set[fs.curr]에 free된 obj 주소를 저장 (TODO: rust에선 이렇게 하는 게 맞나 확인)
    unsafe { *(fs.set.offset(fs.curr as isize)) = obj as usize };
    fs.curr += 1;

    // timestamp 증가 전략이 "free할 때"가 포함이면 timestamp 증가
    if SSMEM_TS_INCR_ON == SSMEM_TS_INCR_ON_FREE || SSMEM_TS_INCR_ON == SSMEM_TS_INCR_ON_BOTH {
        ssmem_ts_next();
    }
}

/// release some memory to the OS using allocator a
pub fn ssmem_release(a: &ssmem_allocator, obj: *mut c_void, pool: Option<&PoolHandle>) {
    todo!("필요하면 구현. SOFT hash 구현에는 안쓰임")
}

/// increment the thread-local activity counter. Invoking this function suggests
/// that no memory references to ssmem-allocated memory are held by the current
/// thread beyond this point. (유저가 이 의미를 가지도록 잘 사용해야함)
pub fn ssmem_ts_next() {
    ssmem_ts_local.with(|ts| {
        let ts_ref = unsafe { ts.borrow_mut().as_mut() }.unwrap();
        ts_ref.version += 1;
    });
}

/* ****************************************************************************************
 */
/* platform-specific definitions */
/* ****************************************************************************************
 */

// TODO: 필요?

/* ****************************************************************************************
 */
/* ssmem.cpp에 있는 global 변수 및 priavte 함수들 */
/* ****************************************************************************************
 */

static mut ssmem_ts_list: *mut ssmem_ts = null_mut(); // TODO: Atomic으로 표현?
static mut ssmem_ts_list_len: usize = 0; // TODO: volatile keyword? Atomic으로 표현?
thread_local! {
    static ssmem_ts_local: RefCell<*mut ssmem_ts> = RefCell::new(null_mut()); // TODO: volatile keyword?
    static ssmem_num_allocators: RefCell<usize>  = RefCell::new(0);
    static ssmem_allocator_list: RefCell<*const ssmem_list> = RefCell::new(null());
}

fn ssmem_list_node_new(
    mem: *mut c_void,
    next: *const ssmem_list,
    pool: Option<&PoolHandle>,
) -> *const ssmem_list {
    let mc: *mut ssmem_list = alloc(Layout::new::<ssmem_list>(), pool);
    let mc_ref = unsafe { mc.as_mut() }.unwrap();
    assert!(!mc.is_null());
    mc_ref.obj = mem;
    mc_ref.next = next;
    mc
}

/// allocator가 쥐고 있는 현재 memory chunk를 zero-initialize
fn ssmem_zero_memory(a: *mut ssmem_allocator) {
    if SSMEM_ZERO_MEMORY == 1 {
        let a_ref = unsafe { a.as_mut() }.unwrap();
        unsafe {
            let _ = libc::memset(a_ref.mem, 0x0, a_ref.mem_size);
        }
        let mut i = 0;
        while i < a_ref.mem_size / CACHE_LINE_SIZE {
            barrier((a_ref.mem as usize + i) as *mut c_void);
            i += CACHE_LINE_SIZE;
        }
    }
}

fn ssmem_free_set_new(
    size: usize,
    next: *mut ssmem_free_set,
    pool: Option<&PoolHandle>,
) -> *mut ssmem_free_set {
    /* allocate both the ssmem_free_set_t and the free_set with one call */
    let mut fs: *mut ssmem_free_set = null_mut();
    fs = alloc(
        Layout::from_size_align(
            size_of::<ssmem_free_set>() + size * size_of::<usize>(),
            CACHE_LINE_SIZE,
        )
        .unwrap(),
        pool,
    );
    assert!(!fs.is_null());

    let fs_ref = unsafe { fs.as_mut() }.unwrap();
    fs_ref.size = size;
    fs_ref.curr = 0;

    fs_ref.set = (fs as usize + size_of::<ssmem_free_set>()) as *mut _; // free obj들이 위치하는 시작 주소
    fs_ref.ts_set = null_mut();
    fs_ref.set_next = next;

    fs
}

/// 다른 스레드의 version들을 `ts_set`(timestamp_set)에 저장해둠. (각 스레드의 version은 ssmem_tx_next에 의해 증가)
fn ssmem_ts_set_collect(ts_set: *mut usize, pool: Option<&PoolHandle>) -> *mut usize {
    let ts_set = if ts_set.is_null() {
        alloc(
            Layout::array::<usize>(unsafe { ssmem_ts_list_len }).unwrap(),
            pool,
        )
    } else {
        ts_set
    };
    assert!(ts_set != null_mut());

    let mut cur = unsafe { ssmem_ts_list.as_mut() };
    while let Some(cur_ref) = cur {
        unsafe { *(ts_set.offset(cur_ref.id as isize)) = cur_ref.version };
        cur = unsafe { cur_ref.next.as_mut() };
    }

    ts_set
}

fn ssmem_mem_reclaim(a: *mut ssmem_allocator, pool: Option<&PoolHandle>) -> isize {
    let a_ref = unsafe { a.as_mut() }.unwrap();

    // 이 if문은 아예 안타는 듯: released_num 증가시키는 ssmem_release를 호출하는 곳이 없음
    if a_ref.released_num > 0 {
        todo!("필요하면 구현. SOFT hash 구현에서는 ssmem_release를 쓰는 곳이 없으므로 이 if문에 진입할 일이 없음")
    }

    let fs_cur = a_ref.free_set_list;
    let fs_cur_ref = unsafe { fs_cur.as_mut() }.unwrap();
    if fs_cur_ref.ts_set.is_null() {
        return 0;
    }
    let fs_nxt = fs_cur_ref.set_next;
    let mut gced_num = 0;
    if fs_nxt.is_null() || unsafe { fs_nxt.as_ref() }.unwrap().ts_set.is_null() {
        // need at least 2 sets to compare
        return 0;
    }
    let fs_nxt_ref = unsafe { fs_nxt.as_mut() }.unwrap();

    if ssmem_ts_compare(fs_cur_ref.ts_set, fs_nxt_ref.ts_set) > 0 {
        // head를 제외하고 모두 garbage collect
        gced_num = a_ref.free_set_num - 1;

        /* take the the suffix of the list (all collected free_sets) away from the
        free_set list of a and set the correct num of free_sets*/
        fs_cur_ref.set_next = null_mut();
        a_ref.free_set_num = 1;

        /* find the tail for the collected_set list in order to append the new
        free_sets that were just collected */
        // head 제외한 free set을 collected set의 tail에 연결
        let mut collected_set_cur = a_ref.collected_set_list;
        if !collected_set_cur.is_null() {
            let mut collected_set_cur_ref = unsafe { collected_set_cur.as_mut() }.unwrap();
            // tail 찾기
            while !collected_set_cur_ref.set_next.is_null() {
                collected_set_cur = collected_set_cur_ref.set_next;
                collected_set_cur_ref = unsafe { collected_set_cur.as_mut() }.unwrap();
            }

            collected_set_cur_ref.set_next = fs_nxt;
        } else {
            a_ref.collected_set_list = fs_nxt;
        }
        a_ref.collected_set_num += gced_num;
    }

    gced_num as isize
}

/// 남는 free set이 있으면 가져다 쓰고(available_set_list), 남는 free set 없으면 새로 만듦
fn ssmem_free_set_get_avail(
    a: *mut ssmem_allocator,
    size: usize,
    next: *mut ssmem_free_set,
    pool: Option<&PoolHandle>,
) -> *mut ssmem_free_set {
    let a_ref = unsafe { a.as_mut() }.unwrap();
    let mut fs = null_mut();

    // 남는 free set 있으므로 재사용
    if !a_ref.available_set_list.is_null() {
        fs = a_ref.available_set_list;
        let fs_ref = unsafe { fs.as_mut() }.unwrap();
        a_ref.available_set_list = fs_ref.set_next;

        fs_ref.curr = 0;
        fs_ref.set_next = next;
    }
    // 남는 free set 없으므로 새로 만듦
    else {
        fs = ssmem_free_set_new(size, next, pool);
    };
    fs
}

/// `set`을 free set으로 재사용할 수 있도록 available list에 추가
fn ssmem_free_set_make_avail(a: *mut ssmem_allocator, set: *mut ssmem_free_set) {
    let a = unsafe { a.as_mut() }.unwrap();
    let set = unsafe { set.as_mut() }.unwrap();
    // set의 curr을 0으로 만들고, available_set_list의 head로 삽입
    set.curr = 0;
    set.set_next = a.available_set_list;
    a.available_set_list = set;
}

fn ssmem_ts_compare(s_new: *const usize, s_old: *const usize) -> usize {
    let len = unsafe { ssmem_ts_list_len };
    let s_new_arr = unsafe { std::slice::from_raw_parts(s_new, len) };
    let s_old_arr = unsafe { std::slice::from_raw_parts(s_old, len) };

    let mut is_newer = 1;
    for i in 0..len {
        if s_new_arr[i] <= s_old_arr[i] {
            is_newer = 0;
            break;
        }
    }
    return is_newer;
}

/// flush + fence
#[inline]
pub fn barrier<T>(p: *const T) {
    debug_assert!(size_of::<T>() <= CACHE_LINE_SIZE);
    clflush(p, CACHE_LINE_SIZE, false);
}

fn alloc<T>(layout: Layout, pool: Option<&PoolHandle>) -> *mut T {
    unsafe {
        return match pool {
            // persistent alloc
            Some(pool) => pool.alloc_layout(layout).deref_mut(pool),
            // volatile alloc
            None => std::alloc::alloc(layout),
        } as *mut T;
    }
}
