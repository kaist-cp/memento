//! SOFT list

use crate::pmem::*;
use crate::PDefault;
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use epoch::{unprotected, Guard};
use libc::c_void;
use std::{
    alloc::Layout,
    cell::RefCell,
    mem::size_of,
    ptr::null_mut,
    sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering},
};

thread_local! {
    /// per-thread persistent ssmem allocator
    // TODO:
    //  - 이 reference는 persistent 해야할듯: 왜냐하면 복구시 crash 이전에 쓰던 durable area과 같은 곳을 가리킬 수 있도록 해야함
    //  - 이게 가능하면 volatile하게 둬도 됨: 복구시 reference를 다시 세팅할 때 crash 이전과 같은 durable area를 가리키게 하기
    // TODO: Ralloc GC시 ssmem_allocator가 가진 memory chunk들은 mark 되게 해야할 듯. 안그러면 Ralloc GC가 ssmem이 사용하던 memory chunk들을 free해감
    static ALLOC: RefCell<*mut ssmem_allocator> = RefCell::new(null_mut());

    /// per-thread volatile ssmem allocator
    // TODO: volatile ssmem allocator는 굳이 필요한가? volatile node는 그냥 Rust standard allocator 써도 되는 거 아닌가?
    static VOLATILE_ALLOC: RefCell<*mut ssmem_allocator> = RefCell::new(null_mut());
}

/// initialize thread-local persistent allocator
fn init_alloc(id: isize, pool: &PoolHandle) {
    let r = pool.alloc::<ssmem_allocator>();
    ALLOC.with(|a| {
        let mut alloc = a.borrow_mut();
        *alloc = unsafe { r.deref_mut(pool) };
        ssmem_alloc_init(*alloc, SSMEM_DEFAULT_MEM_SIZE, id, Some(pool));
    });
}

/// initialize thread-local volatile allocator
fn init_volatile_alloc(id: isize) {
    VOLATILE_ALLOC.with(|a| {
        let mut alloc = a.borrow_mut();
        *alloc =
            unsafe { std::alloc::alloc(Layout::new::<ssmem_allocator>()) as *mut ssmem_allocator };
        ssmem_alloc_init(*alloc, SSMEM_DEFAULT_MEM_SIZE, id, None);
    });
}

/// per-thread initialization
pub fn thread_ini(tid: usize, pool: &PoolHandle) {
    init_alloc(tid as isize, pool);
    init_volatile_alloc(tid as isize)
}

/// TODO: doc
#[derive(Debug)]
pub struct SOFTList<T> {
    head: Atomic<VNode<T>>,
}

impl<T: Default> Default for SOFTList<T> {
    fn default() -> Self {
        let guard = unsafe { unprotected() };
        let head = Atomic::new(VNode::new(0, T::default(), null_mut(), false));
        let head_ref = unsafe { head.load(Ordering::SeqCst, guard).deref() };
        head_ref.next.store(
            Owned::new(VNode::new(usize::MAX, T::default(), null_mut(), false)),
            Ordering::Release,
        );

        Self { head }
    }
}

impl<T: Default> PDefault for SOFTList<T> {
    fn pdefault(_: &PoolHandle) -> Self {
        Self::default()
    }
}

impl<T> Collectable for SOFTList<T> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<T: Clone> SOFTList<T> {
    // TODO: return PPtr<PNode<T>>?
    fn alloc_new_pnode(&self, pool: &PoolHandle) -> *mut PNode<T> {
        ALLOC
            .try_with(|a| {
                let r = ssmem_alloc(*a.borrow_mut(), size_of::<PNode<T>>(), Some(pool));
                r as *mut PNode<T>
            })
            .unwrap()
    }

    // TODO: volatile alloc은 ssmem 안써도 되지 않나?
    fn alloc_new_vnode(
        &self,
        key: usize,
        value: T,
        pptr: *mut PNode<T>,
        p_validity: bool,
    ) -> Owned<VNode<T>> {
        // Owned::new(VNode::new(key, value, pptr, p_validity))

        VOLATILE_ALLOC
            .try_with(|a| {
                let r = ssmem_alloc(*a.borrow_mut(), size_of::<VNode<T>>(), None);
                let mut n = unsafe { Owned::from_raw(r as *mut VNode<T>) };
                n.key = key;
                n.value = value;
                n.pptr = pptr;
                n.p_validity = p_validity;
                n
            })
            .unwrap()
    }

    /// curr을 physical delete
    ///
    /// # Example
    ///
    /// ```text
    /// before: prev --(prev state)--> curr --(curr state: logically deleted)--> succ
    /// after:  prev --(prev state)--> succ
    /// ```
    fn trim(
        &self,
        prev: Shared<'_, VNode<T>>,
        curr: Shared<'_, VNode<T>>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        let prev_state = State::from(curr.tag());
        let prev_ref = unsafe { prev.deref() };
        let curr_ref = unsafe { curr.deref() };
        let succ = curr_ref.next.load(Ordering::SeqCst, guard);
        let result = prev_ref
            .next
            .compare_exchange(
                curr,
                succ.with_tag(prev_state as usize),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .is_ok();
        if result {
            ALLOC
                .try_with(|a| {
                    ssmem_free(*a.borrow_mut(), curr_ref.pptr as *mut c_void, Some(pool));
                })
                .unwrap();
        }
        result
    }

    fn find<'g>(
        &self,
        key: usize,
        currStatePtr: &mut State,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> (Shared<'g, VNode<T>>, Shared<'g, VNode<T>>) {
        let mut prev = self.head.load(Ordering::SeqCst, guard);
        let prev_ref = unsafe { prev.deref() };
        let mut curr = prev_ref.next.load(Ordering::SeqCst, guard);
        let mut curr_ref = unsafe { curr.deref() };
        let mut prev_state = get_state(curr);
        let mut curr_state = State::Dummy; // dummy (TODO: 더 좋은 방법. None을 dummy로 사용?)

        loop {
            let succ = curr_ref.next.load(Ordering::SeqCst, guard);
            let succ_ref = unsafe { succ.deref() }; // TODO: succ는 null일 수 있는데 ㄱㅊ? ref만 얻을뿐 참조는 안하니 괜찮나?
            curr_state = get_state(succ);
            if curr_state != State::Deleted {
                if curr_ref.key >= key {
                    break;
                }
                prev = curr;
                prev_state = curr_state;
            } else {
                let _ = self.trim(prev, curr, guard, pool);
            }
            curr = succ.with_tag(prev_state as usize);
            curr_ref = succ_ref;
        }
        *currStatePtr = curr_state;
        (prev, curr)
    }

    /// TODO: doc
    pub fn insert(&self, key: usize, value: T, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut result = false;
        let mut result_node = None;
        let mut curr_state = State::Dummy;
        'retry: loop {
            let (pred, curr) = self.find(key, &mut curr_state, guard, pool);
            let curr_ref = unsafe { curr.deref() };
            let pred_state = get_state(curr);

            // State: Inserted
            if curr_ref.key == key {
                if curr_state != State::IntendToInsert {
                    // 이미 삽입된 노드. INTEND_TO_INSERT가 아니니 헬핑할 필요도 없음
                    return false;
                }
                // 이 result_node를 helping
                result_node = Some(curr);
            } else {
                let new_pnode = self.alloc_new_pnode(pool);
                let p_valid = unsafe { &mut *new_pnode }.alloc();
                let new_node = self.alloc_new_vnode(key, value.clone(), new_pnode, p_valid);
                new_node.next.store(
                    curr.with_tag(State::IntendToInsert as usize),
                    Ordering::Relaxed,
                );
                let new_node = new_node.into_shared(guard);

                let pred_ref = unsafe { pred.deref() };
                if !pred_ref
                    .next
                    .compare_exchange(
                        curr,
                        new_node.with_tag(pred_state as usize),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    )
                    .is_ok()
                {
                    VOLATILE_ALLOC
                        .try_with(|a| {
                            ssmem_free(*a.borrow_mut(), new_node.as_raw() as *mut c_void, None);
                        })
                        .unwrap();
                    ALLOC
                        .try_with(|a| {
                            ssmem_free(*a.borrow_mut(), new_pnode as *mut c_void, Some(pool));
                        })
                        .unwrap();
                    continue 'retry;
                }
                result_node = Some(new_node);
                result = true;
            }

            // Mark PNode as inserted (durable point)
            let result_node = unsafe { result_node.unwrap().deref() };
            let pptr = unsafe { &mut *result_node.pptr };
            pptr.create(key, value, result_node.p_validity); // 이게 detectable 해야할듯

            // State: IntendToInsert -> Inserted
            loop {
                let next = result_node.next.load(Ordering::SeqCst, guard);
                if get_state(next) != State::IntendToInsert {
                    break;
                }
                let _ = result_node.next.compare_exchange(
                    next,
                    next.with_tag(State::Inserted as usize),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            }
            return result;
        }
    }

    /// TODO: doc
    pub fn remove(&self, key: usize, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut cas_result = false;
        let mut curr_state = State::Dummy;
        let (pred, curr) = self.find(key, &mut curr_state, guard, pool);
        let curr_ref = unsafe { curr.deref() };
        // let pred_state = getState(curr); // TODO: 오타 인듯. 쓰는 곳 없음

        if curr_ref.key != key {
            return false;
        }

        if curr_state == State::IntendToInsert || curr_state == State::Deleted {
            return false;
        }

        // Modify state: INSERTED -> INTEND_TO_DELETE
        while !cas_result
            && get_state(curr_ref.next.load(Ordering::SeqCst, guard)) == State::Inserted
        {
            let next = curr_ref.next.load(Ordering::SeqCst, guard);
            cas_result = curr_ref
                .next
                .compare_exchange(
                    next.with_tag(State::Inserted as usize),
                    next.with_tag(State::IntendToDelete as usize),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                )
                .is_ok();
        }

        // Mark PNode as delete (durable point)
        let pptr = unsafe { &mut *curr_ref.pptr };
        pptr.destroy(curr_ref.p_validity); // 이게 detectable 해야할듯

        // Modify state: INTEND_TO_DELETE -> DELETED (logical delete)
        while get_state(curr_ref.next.load(Ordering::SeqCst, guard)) == State::IntendToDelete {
            let next = curr_ref.next.load(Ordering::SeqCst, guard);
            let _ = curr_ref.next.compare_exchange(
                next.with_tag(State::IntendToDelete as usize),
                next.with_tag(State::Deleted as usize),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );
        }

        // State를 INSERTED에서 INTEND_TO_DELETE로 바꾼 한 명만 physical delete
        if cas_result {
            let _ = self.trim(pred, curr, guard, pool);
        }
        cas_result
    }

    /// TODO: doc
    // TODO: SOFT 본래 구현은 bool 반환하지만 hashEval에선 찾은 T*를 반환함. 왜지? -> 이게 없으면 value를 가져오는 게 없으니까 그런거 같네
    pub fn contains(&self, key: usize, guard: &Guard) -> bool {
        let curr = unsafe { self.head.load(Ordering::SeqCst, guard).deref() }
            .next
            .load(Ordering::SeqCst, guard);
        let mut curr_ref = unsafe { curr.deref() };
        while curr_ref.key < key {
            curr_ref = unsafe { curr_ref.next.load(Ordering::SeqCst, guard).deref() };
        }

        let curr_state = get_state(curr_ref.next.load(Ordering::SeqCst, guard));

        // state가 INSERTED이거나 INTEND_TO_DELETE면 insert된 것
        (curr_ref.key == key)
            && ((curr_state == State::Inserted) || (curr_state == State::IntendToDelete))
    }

    /// recovery용 insert. newPNode에 대한 VNode를 volatile list에 insert함
    fn quick_insert(&self, new_pnode: *mut PNode<T>, guard: &Guard) {
        let new_pnode_ref = unsafe { new_pnode.as_ref() }.unwrap();
        let p_valid = new_pnode_ref.recovery_validity();
        let key = new_pnode_ref.key.load(Ordering::SeqCst);
        let value = unsafe { new_pnode_ref.value.load(Ordering::SeqCst, guard).deref() }.clone();
        let new_node = Owned::new(VNode::new(key, value, new_pnode, p_valid)).into_shared(guard);
        let new_node_ref = unsafe { new_node.deref() };

        let (mut pred, mut curr, mut succ) = (Shared::null(), Shared::null(), Shared::null());
        let mut curr_state = State::Dummy;

        'retry: loop {
            pred = self.head.load(Ordering::SeqCst, guard);
            curr = unsafe { pred.deref() }.next.load(Ordering::SeqCst, guard);
            let mut curr_ref = unsafe { curr.deref() };

            loop {
                succ = curr_ref.next.load(Ordering::SeqCst, guard);
                curr_state = get_state(succ);
                // trimming
                while curr_state == State::Deleted {
                    assert!(false);
                }
                // continue searching
                if curr_ref.key < key {
                    pred = curr;
                    curr = succ;
                    curr_ref = unsafe { curr.deref() };
                }
                // found the same
                else if curr_ref.key == key {
                    assert!(false);
                } else {
                    new_node_ref
                        .next
                        .store(curr.with_tag(State::Inserted as usize), Ordering::Relaxed);
                    let pred_ref = unsafe { pred.deref() };
                    if !pred_ref
                        .next
                        .compare_exchange(
                            curr,
                            new_node.with_tag(State::Inserted as usize),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        continue 'retry;
                    }
                    return;
                }
            }
        }
    }

    // thread가 thread-local durable area를 보고 volatile list에 삽입할 노드를 insert
    // TODO: volatile list를 reconstruct하려면 복구시 per-thread로 이 함수 호출하게 하거나, 혹은 싱글 스레드가 per-thread durable area를 모두 순회하게 해야함
    fn recovery(&self, palloc: &mut ssmem_allocator, pool: &PoolHandle) {
        let mut curr = palloc.mem_chunks;
        while !curr.is_null() {
            let curr_ref = unsafe { curr.as_ref() }.unwrap();
            let curr_chunk = curr_ref.obj as *mut PNode<T>;
            let num_nodes = SSMEM_DEFAULT_MEM_SIZE / size_of::<PNode<T>>();
            for i in 0..num_nodes {
                let curr_node = unsafe { curr_chunk.offset(i as isize) };
                let curr_node_ref = unsafe { curr_node.as_ref() }.unwrap();
                if !curr_node_ref.is_valid() || curr_node_ref.is_deleted() {
                    curr_node_ref.valid_start.store(
                        curr_node_ref.valid_end.load(Ordering::SeqCst),
                        Ordering::SeqCst,
                    );
                    // construct volatile free list of ssmem allocator
                    ssmem_free(palloc, curr_node as *mut c_void, Some(pool));
                } else {
                    // construct volatile SOFT list
                    self.quick_insert(curr_node, &epoch::pin());
                }
            }
            curr = curr_ref.next;
        }
    }
}

/// persistent node
#[repr(align(32))]
#[derive(Debug)]
pub struct PNode<T> {
    valid_start: AtomicBool, // PNode에 key, value write를 시작했는지 여부
    valid_end: AtomicBool, // PNode에 key, value write를 끝냈는지 여부. `valid_start`와 다르면 쓰는 도중이라는 의미
    deleted: AtomicBool,   // PNode가 delete 도ㅒㅆ는지 여부

    // TODO: key, value는 CAS 안쓰는데 왜 Atomic? create시 valid_start, valid_end 사이에 존재하게끔 ordering 보장하려는 목적인가?
    key: AtomicUsize,
    value: Atomic<T>,
}

impl<T> PNode<T> {
    /// PNode의 p_validity값 반환
    // start, end 플래그를 초기값에서 뒤집은 값(p_validity)으로 만드는 게 valid하게 만드는 과정
    fn alloc(&self) -> bool {
        !self.valid_start.load(Ordering::SeqCst)
    }

    /// PNode에 key, value를 쓰고 valid 표시
    fn create(&self, key: usize, value: T, p_validity: bool) {
        self.valid_start.store(p_validity, Ordering::Relaxed);
        fence(Ordering::Release);
        self.key.store(key, Ordering::Relaxed);
        self.value.store(Owned::new(value), Ordering::Relaxed);
        self.valid_end.store(p_validity, Ordering::Release);
        barrier(self);
    }

    /// PNode에 delete 표시
    fn destroy(&self, p_validity: bool) {
        self.deleted.store(p_validity, Ordering::Release);
        barrier(self);
    }

    /// PNode가 valid한 건지 여부 반환
    // start, end가 같고 delete는 다르면 valid한 PNode (i.e. 삽입되어있는 PNode)
    fn is_valid(&self) -> bool {
        (self.valid_start.load(Ordering::SeqCst) == self.valid_end.load(Ordering::SeqCst))
            && self.valid_end.load(Ordering::SeqCst) != self.deleted.load(Ordering::SeqCst)
    }

    /// PNode가 delete된 건지 여부 반환
    // start, end가 같은데 delete만 다르면, delete된 PNode (i.e. 삽입된 후 제거된 PNode)
    fn is_deleted(&self) -> bool {
        (self.valid_start.load(Ordering::SeqCst) == self.valid_end.load(Ordering::SeqCst))
            && self.valid_end.load(Ordering::SeqCst) == self.deleted.load(Ordering::SeqCst)
    }

    fn recovery_validity(&self) -> bool {
        self.valid_start.load(Ordering::SeqCst)
    }
}

/// volatile node
#[derive(Debug)]
struct VNode<T> {
    key: usize,
    value: T,
    pptr: *mut PNode<T>,
    p_validity: bool,
    next: Atomic<VNode<T>>,
}

unsafe impl<T> Sync for VNode<T> {}
unsafe impl<T> Send for VNode<T> {}

impl<T> VNode<T> {
    fn new(key: usize, value: T, pptr: *mut PNode<T>, p_validity: bool) -> Self {
        Self {
            key,
            value,
            pptr,
            p_validity,
            next: Atomic::null(),
        }
    }
}

#[derive(PartialEq, Clone, Copy)]
enum State {
    Inserted,
    IntendToDelete,
    IntendToInsert,
    Deleted,
    Dummy,
}

impl From<usize> for State {
    fn from(tag: usize) -> Self {
        match tag {
            0 => Self::Inserted,
            1 => Self::IntendToDelete,
            2 => Self::IntendToInsert,
            3 => Self::Deleted,
            _ => panic!("invalid cast"),
        }
    }
}

/// 노드의 state 태그를 반환 (helper function)
#[inline]
fn get_state<T>(p: Shared<'_, VNode<T>>) -> State {
    State::from(p.tag())
}
#[cfg(test)]
mod test {
    use epoch::Guard;
    use lazy_static::*;
    use std::sync::{Arc, Barrier};

    use crate::{
        ds::soft_list::{init_alloc, init_volatile_alloc},
        pmem::{Collectable, GarbageCollection, PoolHandle, RootObj},
        test_utils::tests::{run_test, TestRootObj},
    };
    use crossbeam_epoch::{self as epoch};

    use super::{thread_ini, SOFTList};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    lazy_static! {
        static ref BARRIER: Arc<Barrier> = Arc::new(Barrier::new(NR_THREAD));
    }

    #[derive(Debug, Default)]
    struct Smoke {}

    impl Collectable for Smoke {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl RootObj<Smoke> for TestRootObj<SOFTList<usize>> {
        fn run(&self, mmt: &mut Smoke, tid: usize, guard: &Guard, pool: &PoolHandle) {
            // per-thread init
            let barrier = BARRIER.clone();
            thread_ini(tid, pool);
            let _ = barrier.wait();

            // insert, check
            let list = &self.obj;
            for i in 0..COUNT {
                let _ = list.insert(i, tid, guard, pool);
                let _ = list.insert(i + COUNT, tid, guard, pool);
                assert!(list.contains(i, guard));
                assert!(list.contains(i + COUNT, guard));
            }
        }
    }

    #[test]
    fn insert_contain() {
        const FILE_NAME: &str = "soft_list_smoke.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<SOFTList<usize>>, InsertContainRemove, _>(
            FILE_NAME, FILE_SIZE, NR_THREAD,
        )
    }

    #[derive(Debug, Default)]
    struct InsertContainRemove {}

    impl Collectable for InsertContainRemove {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl RootObj<InsertContainRemove> for TestRootObj<SOFTList<usize>> {
        fn run(&self, mmt: &mut InsertContainRemove, tid: usize, guard: &Guard, pool: &PoolHandle) {
            // per-thread init
            let barrier = BARRIER.clone();
            thread_ini(tid, pool);
            let _ = barrier.wait();

            // insert, check, remove, check
            let list = &self.obj;
            for _ in 0..COUNT {
                assert!(list.insert(tid, tid, guard, pool));
                assert!(list.contains(tid, guard));
                assert!(list.remove(tid, guard, pool));
                assert!(!list.contains(tid, guard));
            }
        }
    }

    #[test]
    fn insert_contain_remove() {
        const FILE_NAME: &str = "soft_list_insert_contain_remmove.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<SOFTList<usize>>, InsertContainRemove, _>(
            FILE_NAME, FILE_SIZE, NR_THREAD,
        )
    }
}
