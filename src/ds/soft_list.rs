//! SOFT list

use crate::{pepoch::PShared, pmem::*};
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use epoch::unprotected;
use libc::c_void;
use std::{
    alloc::Layout,
    cell::RefCell,
    mem::size_of,
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering},
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
        // 이제 client가 들고 있을 수 있으니 여기서 free하면 안됨. 대신 delete client가 reset시 자신이 deleter면 free
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
    // TODO: 복구로직
    pub fn insert(&self, key: usize, value: T, client: &mut Insert<T>, pool: &PoolHandle) -> bool {
        // 이미 결과 찍힌 client라면 같은 결과를 반환
        if let Some(res) = client.result() {
            return res;
        }

        // 수행했던 PNode가 있다면, inserter가 나인지를 확인하여 결과 반환
        if !client.target.is_null() {
            let res = unsafe { client.target.deref(pool) }.inserter() == client.id(pool);
            client.set_result(res);
            return res;
        }

        let guard = unsafe { unprotected() }; // free할 노드는 ssmem의 ebr에 의해 관리되기 때문에 crossbeam ebr의 guard는 필요없음
        let mut _result_node = None; // warning 때문에 `_` 붙음
        let mut curr_state = State::Dummy;
        'retry: loop {
            // 삽입할 위치를 탐색
            let (pred, curr) = self.find(key, &mut curr_state);
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
            client.target = PShared::from(unsafe { pnode.as_pptr(pool) });
            persist_obj(&client.target, true);

            // Mark PNode as inserted (durable point)
            let pnode = unsafe { result_node.pptr.as_mut().unwrap() };
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
            return result;
        }
    }

    /// TODO: doc
    // TODO: 복구로직
    pub fn remove(&self, key: usize, client: &mut Remove<T>, pool: &PoolHandle) -> bool {
        // 이미 수행한 client라면 같은 결과를 반환
        if let Some(res) = client.result() {
            return res;
        }

        // 수행했던 PNode가 있다면, deleter가 나인지를 확인하여 결과 반환
        if !client.target.is_null() {
            let res = unsafe { client.target.deref(pool) }.deleter() == client.id(pool);
            client.set_result(res);
            return res;
        }

        let guard = unsafe { unprotected() }; // free할 노드는 ssmem의 ebr에 의해 관리되기 때문에 crossbeam ebr의 guard는 필요없음
        let mut cas_result = false;
        let mut curr_state = State::Dummy;
        let (pred, curr) = self.find(key, &mut curr_state);
        let curr_ref = unsafe { curr.deref() };

        if curr_ref.key != key {
            client.set_result(false);
            return false;
        }

        if curr_state == State::IntendToInsert || curr_state == State::Deleted {
            client.set_result(false);
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

        // clinet가 PNode를 타겟팅
        let pnode = unsafe { curr_ref.pptr.as_ref().unwrap() };
        client.target = PShared::from(unsafe { pnode.as_pptr(pool) });
        persist_obj(&client.target, true);

        // Mark PNode as delete (durable point)
        let result = pnode.destroy(client, pool);

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
        if result {
            let _ = self.trim(pred, curr);
        }
        result
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
        let key = new_pnode_ref.key;
        let value = new_pnode_ref.value.clone();
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
#[derive(Debug, Default)]
pub struct Insert<T: 'static> {
    target: PShared<'static, PNode<T>>,
}

impl<T> Insert<T> {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }

    fn result(&self) -> Option<bool> {
        match self.target.tag() {
            SUCCEED => Some(true),
            FAILED => Some(false),
            _ => None,
        }
    }

    fn set_result(&mut self, succeed: bool) {
        self.target = self.target.with_tag(if succeed { SUCCEED } else { FAILED });
        persist_obj(&self.target, true);
    }

    /// TODO: doc
    #[inline]
    pub fn reset(&mut self) {
        self.target = PShared::null();
        persist_obj(&self.target, true);
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct Remove<T: 'static> {
    target: PShared<'static, PNode<T>>,
}

impl<T: PartialEq + Clone> Remove<T> {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }

    fn result(&self) -> Option<bool> {
        match self.target.tag() {
            SUCCEED => Some(true),
            FAILED => Some(false),
            _ => None,
        }
    }

    fn set_result(&mut self, succeed: bool) {
        self.target = self.target.with_tag(if succeed { SUCCEED } else { FAILED });
        persist_obj(&self.target, true);
    }

    /// TODO: doc
    #[inline]
    pub fn reset(&mut self) {
        let target = self.target;
        self.target = PShared::null();
        persist_obj(&self.target, true);

        // target의 deleter가 나라면, 내가 free
        if !target.is_null() {
            let pool = global_pool().unwrap();
            let pnode_ref = unsafe { target.deref(pool) };

            if pnode_ref.deleter() == self.id(pool) {
                ALLOC
                    .try_with(|a| {
                        ssmem_free(
                            *a.borrow_mut(),
                            pnode_ref as *const _ as *mut c_void,
                            Some(pool),
                        );
                    })
                    .unwrap();
            }
        }
    }
}

// client의 결과를 나타낼 tag
const SUCCEED: usize = 1;
const FAILED: usize = 2;

/// persistent node
#[repr(align(32))]
#[derive(Debug)]
struct PNode<T> {
    /// PNode가 "삽입완료" 됐는지 여부. true면 "삽입완료"를 의미
    inserted: bool,

    /// PNode를 삽입한 client(의 상대주소)
    inserter: AtomicUsize,

    // PNode를 삭제한 client(의 상대주소). 0이 아니면 "삭제완료"를 의미
    deleter: AtomicUsize,

    // TODO: key, value는 CAS 안쓰는데 왜 Atomic? create시 valid_start, valid_end 사이에 존재하게끔 ordering 보장하려는 목적인가?
    key: usize,
    value: T,
}

impl<T: Clone + PartialEq> PNode<T> {
    /// PNode에 key, value를 쓰고 valid 표시
    fn create(
        &mut self,
        key: usize,
        value: T,                 // PNode에 쓰일 value
        inserter: &mut Insert<T>, // client
        inserter_value: T,        // client가 시도하려던 value
        pool: &PoolHandle,
    ) -> bool {
        self.key = key;
        self.value = value.clone();
        self.inserted = true;
        // TODO: persist 추가? (i.e. inserted -> inserter order가 중요한가?)
        let res = if value == inserter_value {
            self.inserter
                .compare_exchange(0, inserter.id(pool), Ordering::Release, Ordering::Relaxed)
                .is_ok()
        } else {
            false
        };
        persist_obj(self, true);
        res
    }

    /// PNode에 delete 표시
    fn destroy(&self, remover: &mut Remove<T>, pool: &PoolHandle) -> bool {
        let res = self
            .deleter
            .compare_exchange(0, remover.id(pool), Ordering::Release, Ordering::Relaxed)
            .is_ok();
        persist_obj(self, true);
        res
    }

    /// PNode가 valid한 건지(i.e. 삽입되어있는 건지) 여부 반환
    fn is_valid(&self) -> bool {
        self.inserter() != 0 && self.deleter() == 0
    }

    /// PNode가 delete된 건지 여부 반환
    // TODO: correctness
    // - inserter, deleter 둘다 0인 것만 재사용 가능한 PNode block으로 취급.
    // - inserter, deleter 둘다 있는 건 VList엔 없지만 Client가 들고 있는 노드임. allocator가 복구시 미사용중인 PNode로 판별하고 free block으로 가져가면 안됨.
    // TODO: leak-free
    //  1  ssmem의 ebr이 collect하여 재사용가능한 block은 zero-initialze 해줘야할 듯 (새로 alloc 받은 것이 zero-initialize 되어있기만 하면 됨)
    //  2. allocator는 복구시 VList를 재구성하기 전에 (1) inserter, deleter 둘다 찍혀있지만 (2) 이를 가리키는 client가 없는 block들을 찾아서 zero-initialize 부터 해줘야할 듯
    fn is_deleted(&self) -> bool {
        self.inserter() != 0 && self.deleter() != 0
    }

    fn inserter(&self) -> usize {
        self.inserter.load(Ordering::SeqCst)
    }

    fn deleter(&self) -> usize {
        self.deleter.load(Ordering::SeqCst)
    }
}

unsafe impl<T> Sync for PShared<'_, PNode<T>> {}
unsafe impl<T> Send for PShared<'_, PNode<T>> {}

/// volatile node
#[derive(Debug)]
struct VNode<T> {
    key: usize,
    value: T,
    pptr: *mut PNode<T>,
    next: Atomic<VNode<T>>,
}

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

unsafe impl<T> Sync for VNode<T> {}
unsafe impl<T> Send for VNode<T> {}

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

    use super::{thread_ini, Insert, Remove, SOFTList};

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
        remove: Remove<usize>,
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
            let insert_cli = &mut client.insert;
            let remove_cli = &mut client.remove;
            for _ in 0..COUNT {
                assert!(list.insert(tid, tid, insert_cli, pool));
                assert!(list.contains(tid));
                assert!(list.remove(tid, remove_cli, pool));
                assert!(!list.contains(tid));
                insert_cli.reset();
                remove_cli.reset();
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
