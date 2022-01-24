//! SOFT list

use crate::pmem::*;
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use epoch::unprotected;
use libc::c_void;
use std::{
    alloc::Layout,
    cell::RefCell,
    mem::size_of,
    ptr::null_mut,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

thread_local! {
    /// per-thread persistent ssmem allocator
    // TODO:
    //  - 이 reference는 persistent 해야할듯: 왜냐하면 복구시 crash 이전에 쓰던 durable area과 같은 곳을 가리킬 수 있도록 해야함
    //  - 이게 가능하면 volatile하게 둬도 됨: 복구시 reference를 다시 세팅할 때 crash 이전과 같은 durable area를 가리키게 하기
    // TODO: Ralloc GC시 ssmem_allocator가 가진 memory chunk들은 mark 되게 해야할 듯. 안그러면 Ralloc GC가 ssmem이 사용하던 memory chunk들을 free해감
    static ALLOC: RefCell<*mut SsmemAllocator> = RefCell::new(null_mut());

    /// per-thread volatile ssmem allocator
    // TODO: volatile ssmem allocator는 굳이 필요한가? volatile node는 그냥 Rust standard allocator 써도 되는 거 아닌가?
    static VOLATILE_ALLOC: RefCell<*mut SsmemAllocator> = RefCell::new(null_mut());
}

/// initialize thread-local persistent allocator
fn init_alloc(id: isize, pool: &PoolHandle) {
    let r = pool.alloc::<SsmemAllocator>();
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
            unsafe { std::alloc::alloc(Layout::new::<SsmemAllocator>()) as *mut SsmemAllocator };
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
        // head, tail sentinel 노드 삽입. head, tail은 free되지 않으며 다른 노드는 둘의 사이에 삽입됐다 빠졌다함
        let guard = unsafe { unprotected() };
        let head = Atomic::new(VNode::new(0, T::default(), null_mut()));
        let head_ref = unsafe { head.load(Ordering::SeqCst, guard).deref() };
        head_ref.next.store(
            Owned::new(VNode::new(usize::MAX, T::default(), null_mut())),
            Ordering::Release,
        );

        Self { head }
    }
}

impl<T: Clone + PartialEq> SOFTList<T> {
    // TODO: PNode는 alloc 받았을 때 zero-initialized 돼있어야함. ssmem 구현보니 free obj를 재사용할때 zero-initialized 추가해야할듯
    fn alloc_new_pnode(&self, pool: &PoolHandle) -> *mut PNode<T> {
        ALLOC
            .try_with(|a| {
                let r = ssmem_alloc(*a.borrow_mut(), size_of::<PNode<T>>(), Some(pool));
                r as *mut PNode<T>
            })
            .unwrap()
    }

    fn alloc_new_vnode(&self, key: usize, value: T, pptr: *mut PNode<T>) -> Shared<'_, VNode<T>> {
        VOLATILE_ALLOC
            .try_with(|a| {
                let r = ssmem_alloc(*a.borrow_mut(), size_of::<VNode<T>>(), None);
                let mut n = unsafe { Owned::from_raw(r as *mut VNode<T>) };
                n.key = key;
                n.value = value;
                n.pptr = pptr;
                n.into_shared(unsafe { unprotected() })
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
    fn trim(&self, prev: Shared<'_, VNode<T>>, curr: Shared<'_, VNode<T>>) -> bool {
        let guard = unsafe { unprotected() }; // free할 노드는 ssmem의 ebr에 의해 관리되기 때문에 crossbeam ebr의 guard는 필요없음
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
        // 이제 client가 들고 있을 수 있으니 free하면 안됨. TODO: delete client가 reset시 자신이 deleter면 free
        // if result {
        //     ALLOC
        //         .try_with(|a| {
        //             ssmem_free(*a.borrow_mut(), curr_ref.pptr as *mut c_void, Some(pool));
        //         })
        //         .unwrap();
        // }
        result
    }

    fn find<'g>(
        &self,
        key: usize,
        curr_state_ptr: &mut State,
        pool: &PoolHandle,
    ) -> (Shared<'g, VNode<T>>, Shared<'g, VNode<T>>) {
        let guard = unsafe { unprotected() }; // free할 노드는 ssmem의 ebr에 의해 관리되기 때문에 crossbeam ebr의 guard는 필요없음
        let mut prev = self.head.load(Ordering::SeqCst, guard);
        let prev_ref = unsafe { prev.deref() };
        let mut curr = prev_ref.next.load(Ordering::SeqCst, guard);
        let mut curr_ref = unsafe { curr.deref() };
        let mut prev_state = get_state(curr);
        let mut _curr_state = State::Dummy; // // warning 때문에 `_` 붙음

        loop {
            let succ = curr_ref.next.load(Ordering::SeqCst, guard);
            let succ_ref = unsafe { succ.deref() };
            _curr_state = get_state(succ);
            if _curr_state != State::Deleted {
                if curr_ref.key >= key {
                    break;
                }
                prev = curr;
                prev_state = _curr_state;
            } else {
                let _ = self.trim(prev, curr);
            }
            curr = succ.with_tag(prev_state as usize);
            curr_ref = succ_ref;
        }
        *curr_state_ptr = _curr_state;
        (prev, curr)
    }

    /// TODO: doc
    pub fn insert(&self, key: usize, value: T, client: &mut Insert<T>, pool: &PoolHandle) -> bool {
        // 이미 수행한 client라면 같은 결과를 반환
        if let Some(res) = client.result() {
            return res;
        }

        let guard = unsafe { unprotected() }; // free할 노드는 ssmem의 ebr에 의해 관리되기 때문에 crossbeam ebr의 guard는 필요없음
        let mut _result_node = None; // warning 때문에 `_` 붙음
        let mut curr_state = State::Dummy;
        'retry: loop {
            // 삽입할 위치를 탐색
            let (pred, curr) = self.find(key, &mut curr_state, pool);
            let curr_ref = unsafe { curr.deref() };
            let pred_state = get_state(curr);

            // 중복 키를 발견. 삽입 중이라면 helping하고, 삽입 완료된 거면 그냥 끝냄
            if curr_ref.key == key {
                if curr_state != State::IntendToInsert {
                    // 이미 삽입된 노드. INTEND_TO_INSERT가 아니니 헬핑할 필요도 없음
                    // "실패"로 끝났음을 표시
                    client.set_result(false);
                    return false;
                }
                // 이 result_node를 helping
                _result_node = Some(curr);
            }
            // 중복 키 없으므로 State: IntendToInsert 노드를 만들어 삽입 시도
            else {
                let new_pnode = self.alloc_new_pnode(pool);
                let new_node = self.alloc_new_vnode(key, value.clone(), new_pnode);
                unsafe { new_node.deref() }.next.store(
                    curr.with_tag(State::IntendToInsert as usize),
                    Ordering::Relaxed,
                );

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
                    // 삽입 실패시 alloc 했던거 free하고 처음부터 재시도
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
                _result_node = Some(new_node);
            }
            let result_node = unsafe { _result_node.unwrap().deref() };

            // clinet가 PNode를 타겟팅
            let pnode = unsafe { result_node.pptr.as_ref().unwrap() };
            client.target = unsafe { pnode.as_pptr(pool) };
            persist_obj(&client.target, true);

            // Mark PNode as inserted (durable point)
            let result = pnode.create(
                result_node.key,
                result_node.value.clone(),
                client,
                value,
                pool,
            );

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

            client.set_result(result);
            return result;
        }
    }

    /// TODO: doc
    // TODO: detectable 버전으로 변경
    pub fn remove(&self, key: usize, pool: &PoolHandle) -> bool {
        let guard = unsafe { unprotected() }; // free할 노드는 ssmem의 ebr에 의해 관리되기 때문에 crossbeam ebr의 guard는 필요없음
        let mut cas_result = false;
        let mut curr_state = State::Dummy;
        let (pred, curr) = self.find(key, &mut curr_state, pool);
        let curr_ref = unsafe { curr.deref() };
        // let pred_state = getState(curr); // SOFT 본래 구현엔 있지만 오타 인듯. 쓰는 곳 없음

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
        let deleter = true; // TODO: delete client
        let pptr = unsafe { &mut *curr_ref.pptr };
        pptr.destroy(deleter);

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
            let _ = self.trim(pred, curr);
        }
        cas_result
    }

    /// TODO: doc
    // TODO: SOFT 본래 구현은 bool 반환하지만 hashEval에선 찾은 T*를 반환함. 왜지? -> 이게 없으면 value를 가져오는 게 없으니까 그런거 같네
    pub fn contains(&self, key: usize) -> bool {
        let guard = unsafe { unprotected() }; // free할 노드는 ssmem의 ebr에 의해 관리되기 때문에 crossbeam ebr의 guard는 필요없음
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
    // TODO: 다른 거 detectable 버전으로 변경 완료되면 이 로직 다시 확인
    #[allow(unused)]
    fn quick_insert(&self, new_pnode: *mut PNode<T>) {
        let guard = unsafe { unprotected() }; // free할 노드는 ssmem의 ebr에 의해 관리되기 때문에 crossbeam ebr의 guard는 필요없음
        let new_pnode_ref = unsafe { new_pnode.as_ref() }.unwrap();
        let key = new_pnode_ref.key.load(Ordering::SeqCst);
        let value = unsafe { new_pnode_ref.value.load(Ordering::SeqCst, guard).deref() }.clone();
        let new_node = Owned::new(VNode::new(key, value, new_pnode)).into_shared(guard);
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
    // TODO: 다른 거 detectable 버전으로 변경 완료되면 이 로직 다시 확인
    #[allow(unused)]
    fn recovery(&self, palloc: &mut SsmemAllocator, pool: &PoolHandle) {
        let mut curr = palloc.mem_chunks;
        while !curr.is_null() {
            let curr_ref = unsafe { curr.as_ref() }.unwrap();
            let curr_chunk = curr_ref.obj as *mut PNode<T>;
            let num_nodes = SSMEM_DEFAULT_MEM_SIZE / size_of::<PNode<T>>();
            for i in 0..num_nodes {
                let curr_node = unsafe { curr_chunk.offset(i as isize) };
                let curr_node_ref = unsafe { curr_node.as_ref() }.unwrap();
                if !curr_node_ref.is_valid() || curr_node_ref.is_deleted() {
                    // construct volatile free list of ssmem allocator
                    ssmem_free(palloc, curr_node as *mut c_void, Some(pool));
                } else {
                    // construct volatile SOFT list
                    self.quick_insert(curr_node);
                }
            }
            curr = curr_ref.next;
        }
    }
}

/// client for insert or remove
#[derive(Debug)]
pub struct Insert<T> {
    target: PPtr<PNode<T>>,
}

impl<T> Default for Insert<T> {
    fn default() -> Self {
        Self {
            target: PPtr::from(ClientState::NotFinished),
        }
    }
}

impl<T> Insert<T> {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }

    fn set_result(&mut self, succeed: bool) {
        if succeed {
            self.target = PPtr::from(ClientState::Succeed)
        } else {
            self.target = PPtr::from(ClientState::Failed)
        }
        persist_obj(&self.target, true);
    }

    fn result(&self) -> Option<bool> {
        if self.target == PPtr::from(ClientState::Failed) {
            Some(false)
        } else if self.target == PPtr::from(ClientState::Succeed) {
            Some(true)
        } else {
            None
        }
    }

    /// TODO: doc
    #[inline]
    pub fn reset(&mut self) {
        self.target = PPtr::from(ClientState::NotFinished);
        persist_obj(&self.target, true);
    }
}

enum ClientState {
    NotFinished,
    Failed,
    Succeed,
}

impl<T> From<ClientState> for PPtr<T> {
    fn from(state: ClientState) -> Self {
        PPtr::from(state as usize)
    }
}

/// persistent node
#[repr(align(32))]
#[derive(Debug)]
pub struct PNode<T> {
    /// PNode를 insert한 client(의 상대주소)
    inserter: AtomicUsize,

    /// PNode가 delete 도ㅒㅆ는지 여부
    // TODO: deleter: AtomicUsize로 대체 (PNode를 delete한 client(의 상대주소)
    deleted: AtomicBool, // PNode가 delete 도ㅒㅆ는지 여부

    // TODO: key, value는 CAS 안쓰는데 왜 Atomic? create시 valid_start, valid_end 사이에 존재하게끔 ordering 보장하려는 목적인가?
    key: AtomicUsize,
    value: Atomic<T>,
}

impl<T: PartialEq> PNode<T> {
    /// PNode에 key, value를 쓰고 valid 표시
    fn create(
        &self,
        key: usize,
        value: T,                 // PNode에 쓰일 value
        inserter: &mut Insert<T>, // client
        inserter_value: T,        // client가 시도하려던 value
        pool: &PoolHandle,
    ) -> bool {
        let res = if value == inserter_value {
            self.key.store(key, Ordering::Relaxed);
            self.value.store(Owned::new(value), Ordering::Relaxed);
            self.inserter
                .compare_exchange(0, inserter.id(pool), Ordering::Release, Ordering::Relaxed)
                .is_ok()
        } else {
            self.key.store(key, Ordering::Relaxed);
            self.value.store(Owned::new(value), Ordering::Release);
            false
        };
        persist_obj(self, true);
        res
    }

    /// PNode에 delete 표시
    fn destroy(&self, p_validity: bool) {
        self.deleted.store(p_validity, Ordering::Release);
        persist_obj(self, true);
    }

    /// PNode가 valid한 건지 여부 반환
    // start, end가 같고 delete는 다르면 valid한 PNode (i.e. 삽입되어있는 PNode)
    fn is_valid(&self) -> bool {
        self.inserter.load(Ordering::SeqCst) != 0 && !self.deleted.load(Ordering::SeqCst)
    }

    /// PNode가 delete된 건지 여부 반환
    // start, end가 같은데 delete만 다르면, delete된 PNode (i.e. 삽입된 후 제거된 PNode)
    fn is_deleted(&self) -> bool {
        self.inserter.load(Ordering::SeqCst) != 0 && self.deleted.load(Ordering::SeqCst)
    }
}

/// volatile node
#[derive(Debug)]
struct VNode<T> {
    key: usize,
    value: T,
    pptr: *mut PNode<T>,
    next: Atomic<VNode<T>>,
}

unsafe impl<T> Sync for VNode<T> {}
unsafe impl<T> Send for VNode<T> {}

impl<T> VNode<T> {
    fn new(key: usize, value: T, pptr: *mut PNode<T>) -> Self {
        Self {
            key,
            value,
            pptr,
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
#[allow(box_pointers)]
mod test {
    use epoch::Guard;
    use lazy_static::*;
    use std::sync::{Arc, Barrier};

    use crate::{
        pmem::{Collectable, GarbageCollection, PoolHandle, RootObj},
        test_utils::tests::{run_test, TestRootObj},
        PDefault,
    };
    use crossbeam_epoch::{self as epoch};

    use super::{thread_ini, Insert, SOFTList};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100000;

    lazy_static! {
        static ref BARRIER: Arc<Barrier> = Arc::new(Barrier::new(NR_THREAD));
    }

    struct SOFTListRoot {
        list: Box<SOFTList<usize>>,
    }

    impl PDefault for SOFTListRoot {
        fn pdefault(_: &PoolHandle) -> Self {
            Self {
                list: Box::new(SOFTList::default()),
            }
        }
    }

    impl Collectable for SOFTListRoot {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
            todo!()
        }
    }
    #[derive(Debug, Default)]
    struct InsertContainRemove {
        insert: Insert<usize>,
    }

    impl Collectable for InsertContainRemove {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
            todo!()
        }
    }

    impl RootObj<InsertContainRemove> for TestRootObj<SOFTListRoot> {
        fn run(&self, client: &mut InsertContainRemove, tid: usize, _: &Guard, pool: &PoolHandle) {
            // per-thread init
            let barrier = BARRIER.clone();
            thread_ini(tid, pool);
            let _ = barrier.wait();

            // insert, check, remove, check
            let list = &self.obj.list;
            let insert_client = &mut client.insert;
            for _ in 0..COUNT {
                assert!(list.insert(tid, tid, insert_client, pool));
                assert!(list.contains(tid));
                assert!(list.remove(tid, pool));
                assert!(!list.contains(tid));
                insert_client.reset();
            }
        }
    }

    #[test]
    fn insert_contain_remove() {
        const FILE_NAME: &str = "soft_list_insert_contain_remmove.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<SOFTListRoot>, InsertContainRemove, _>(
            FILE_NAME, FILE_SIZE, NR_THREAD,
        )
    }
}
