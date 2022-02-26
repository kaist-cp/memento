//! Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
#![allow(non_snake_case)]
use crate::ds::spin_lock_volatile::VSpinLock;
use crate::pepoch::atomic::Pointer;
use crate::pepoch::{unprotected, PAtomic, PDestroyable, POwned};
use crate::ploc::Checkpoint;
use crate::pmem::{persist_obj, sfence, Collectable, GarbageCollection, PPtr, PoolHandle};
use crate::PDefault;
use array_init::array_init;
use crossbeam_epoch::Guard;
use crossbeam_utils::{Backoff, CachePadded};
use etrace::ok_or;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tinyvec::tiny_vec;

const MAX_THREADS: usize = 32;
type Data = usize; // TODO: generic

/// 사용할 스레드 수. combining시 이 스레드 수만큼만 op 순회
pub static mut NR_THREADS: usize = MAX_THREADS;

type EnqRetVal = ();
type DeqRetVal = Option<Data>;

/// client for enqueue
#[derive(Debug, Default)]
pub struct Enqueue {
    req: Checkpoint<PAtomic<EnqRequestRec>>,
    result: Checkpoint<EnqRetVal>,
}

impl Collectable for Enqueue {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut enq.req, tid, gc, pool);
        Checkpoint::filter(&mut enq.result, tid, gc, pool);
    }
}

/// client for dequeue
#[derive(Debug, Default)]
pub struct Dequeue {
    req: Checkpoint<PAtomic<DeqRequestRec>>,
    result: Checkpoint<DeqRetVal>,
}

impl Collectable for Dequeue {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut deq.req, tid, gc, pool);
        Checkpoint::filter(&mut deq.result, tid, gc, pool);
    }
}

#[derive(Debug, Default)]
struct EnqRequestRec {
    arg: usize,
    activate: AtomicBool,
}

impl Collectable for EnqRequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

#[derive(Debug, Default)]
struct DeqRequestRec {
    activate: AtomicBool,
}

impl Collectable for DeqRequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

/// Node
#[derive(Debug, Default)]
pub struct Node {
    data: Data,
    next: PAtomic<Node>, // NOTE: reordering 방지를 위한 atomic. CAS는 안씀
}

impl Collectable for Node {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.next, tid, gc, pool);
    }
}

/// State of Enqueue PBComb
#[derive(Debug)]
struct EStateRec {
    tail: PAtomic<Node>, // NOTE: reordering 방지를 위한 atomic. CAS는 안씀
    return_val: [EnqRetVal; MAX_THREADS + 1],
    deactivate: [AtomicBool; MAX_THREADS + 1],
}

impl Collectable for EStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.tail, tid, gc, pool);
    }
}

impl Clone for EStateRec {
    fn clone(&self) -> Self {
        Self {
            tail: self.tail.clone(),
            return_val: self.return_val.clone(),
            deactivate: array_init(|i| AtomicBool::new(self.deactivate[i].load(Ordering::SeqCst))),
        }
    }
}

/// State of Dequeue PBComb
#[derive(Debug)]
struct DStateRec {
    head: PAtomic<Node>, // NOTE: reordering 방지를 위한 atomic. CAS는 안씀
    return_val: [DeqRetVal; MAX_THREADS + 1],
    deactivate: [AtomicBool; MAX_THREADS + 1],
}

impl Clone for DStateRec {
    fn clone(&self) -> Self {
        Self {
            head: self.head.clone(),
            return_val: self.return_val.clone(),
            deactivate: array_init(|i| AtomicBool::new(self.deactivate[i].load(Ordering::SeqCst))),
        }
    }
}

impl Collectable for DStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.head, tid, gc, pool);
    }
}

// Shared volatile variables
lazy_static::lazy_static! {
    /// 현재 진행중인 enq combiner가 enq 시작한 지점. 여기서부턴 persist아직 보장되지 않았으니 deq해가면 안됨
    ///
    /// - 프로그램 시작시 dummy node를 가리키도록 초기화 (첫 시작은 pdefault에서, 이후엔 gc에서)
    /// - 노드를 직접 저장하지 않고 노드의 상대주소를 저장 (여기에 Atomic<PPtr<Node>>나 PAtomic<Node>는 이상함)
    static ref OLD_TAIL: AtomicUsize = AtomicUsize::new(0);

    /// Used by the PBQueueENQ instance of PBCOMB
    static ref E_LOCK: VSpinLock = VSpinLock::default();
    static ref E_DEACTIVATE_LOCK: [AtomicUsize; MAX_THREADS + 1] = array_init(|_| AtomicUsize::new(0)); // TODO: 더 적절한 이름..

    /// Used by the PBQueueDEQ instance of PBCOMB
    static ref D_LOCK: VSpinLock = VSpinLock::default();
    static ref D_DEACTIVATE_LOCK: [AtomicUsize; MAX_THREADS + 1] = array_init(|_| AtomicUsize::new(0));
}

/// TODO: doc
#[derive(Debug)]
pub struct Queue {
    /// Shared non-volatile variables
    dummy: PPtr<Node>,

    /// Shared non-volatile variables used by the PBQueueENQ instance of PBCOMB
    // NOTE: enq하는데 deq의 variable을 쓰는 실수 주의
    e_request: [CachePadded<PAtomic<EnqRequestRec>>; MAX_THREADS + 1],
    e_state: [CachePadded<EStateRec>; 2],
    e_index: AtomicUsize,

    /// Shared non-volatile variables used by the PBQueueDEQ instance of PBCOMB
    d_request: [CachePadded<PAtomic<DeqRequestRec>>; MAX_THREADS + 1],
    d_state: [CachePadded<DStateRec>; 2],
    d_index: AtomicUsize,
}

impl Collectable for Queue {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PPtr::filter(&mut s.dummy, tid, gc, pool);
        for tid in 0..MAX_THREADS + 1 {
            PAtomic::filter(&mut *s.e_request[tid], tid, gc, pool);
            PAtomic::filter(&mut *s.d_request[tid], tid, gc, pool);
        }
        EStateRec::filter(&mut *s.e_state[0], tid, gc, pool);
        EStateRec::filter(&mut *s.e_state[1], tid, gc, pool);
        DStateRec::filter(&mut *s.d_state[0], tid, gc, pool);
        DStateRec::filter(&mut *s.d_state[1], tid, gc, pool);

        // initialize global volatile variable manually
        OLD_TAIL.store(s.dummy.into_offset(), Ordering::SeqCst);
    }
}

impl PDefault for Queue {
    fn pdefault(pool: &PoolHandle) -> Self {
        let dummy = pool.alloc::<Node>();
        let dummy_ref = unsafe { dummy.deref_mut(pool) };
        dummy_ref.data = 0;
        dummy_ref.next = PAtomic::null();

        // initialize global volatile variable manually
        OLD_TAIL.store(dummy.into_offset(), Ordering::SeqCst);

        Self {
            dummy,
            e_request: array_init(|_| Default::default()),
            e_state: array_init(|_| {
                CachePadded::new(EStateRec {
                    tail: PAtomic::from(dummy),
                    return_val: array_init(|_| ()),
                    deactivate: array_init(|_| AtomicBool::new(false)),
                })
            }),
            e_index: Default::default(),
            d_request: array_init(|_| Default::default()),
            d_state: array_init(|_| {
                CachePadded::new(DStateRec {
                    head: PAtomic::from(dummy),
                    return_val: array_init(|_| None),
                    deactivate: array_init(|_| AtomicBool::new(false)),
                })
            }),
            d_index: Default::default(),
        }
    }
}

/// Enq
impl Queue {
    /// enq 요청 등록 후 수행
    pub fn PBQueueEnq<const REC: bool>(
        &mut self,
        arg: Data,
        enq: &mut Enqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> EnqRetVal {
        let prev = self.e_request[tid].load(Ordering::SeqCst, guard);
        let prev_activate = if prev.is_null() {
            false
        } else {
            unsafe { prev.deref(pool).activate.load(Ordering::SeqCst) }
        };

        // 새로운 요청 생성
        let req = POwned::new(
            EnqRequestRec {
                arg,
                activate: AtomicBool::new(!prev_activate),
            },
            pool,
        );
        persist_obj(unsafe { req.deref(pool) }, true);

        let req = ok_or!(
            enq.req.checkpoint::<REC>(PAtomic::from(req), tid, pool),
            e,
            unsafe {
                drop(e.new.load(Ordering::Relaxed, unprotected()).into_owned());
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);

        // 이전에 끝난 client라면 같은 결과 반환
        if REC {
            if let Some(res) = enq.result.peek(tid, pool) {
                return res;
            }
        }

        // 요청 저장소에 등록
        // NOTE: 위에 if REC { 결과 반환 } 없애려면 이 요청 등록도(+state도?) checkpoint해야할 듯.
        //       안그러면 이미 끝난 요청이 또 등록되어 수행될 수 있음. 또 수행되면 combiner가 enq 노드 새로할당해서 새로 넣음
        self.e_request[tid].store(req, Ordering::SeqCst);

        // 등록한 요청 수행
        self.PerformEnqReq::<REC>(enq, tid, guard, pool)
    }

    /// 요청 수행
    fn PerformEnqReq<const REC: bool>(
        &mut self,
        enq: &mut Enqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> EnqRetVal {
        // enq combiner 결정
        let (lval, lockguard) = loop {
            // combiner 되기를 시도. lval을 내가 점유했다면 내가 combiner
            let lval = match E_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret,
                Err((lval, _)) => lval,
            };

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            let backoff = Backoff::new();
            if lval % 2 == 1 {
                while lval == E_LOCK.peek().0 {
                    backoff.snooze();
                }
            }

            if unsafe {
                self.e_request[tid]
                    .load(Ordering::SeqCst, guard)
                    .deref(pool)
            }
            .activate
            .load(Ordering::SeqCst)
                == self.e_state[self.e_index.load(Ordering::SeqCst)].deactivate[tid]
                    .load(Ordering::SeqCst)
            {
                // 자신의 op을 처리한 combiner가 끝날때까지 기다렸다가 결과 반환
                let deactivate_lval = E_DEACTIVATE_LOCK[tid].load(Ordering::SeqCst);
                backoff.reset();
                while !(deactivate_lval < E_LOCK.peek().0) {
                    backoff.snooze();
                }

                // 결과 저장하고 반환
                let res = self.e_state[self.e_index.load(Ordering::SeqCst)].return_val[tid].clone();
                let _ = enq.result.checkpoint::<REC>(res, tid, pool);
                return res;
            }
        };

        // enq combiner는 쌓인 enq 요청들을 수행
        let ind = 1 - self.e_index.load(Ordering::SeqCst);
        self.e_state[ind] = self.e_state[self.e_index.load(Ordering::SeqCst)].clone(); // create a copy of current state
        OLD_TAIL.store(
            self.e_state[ind]
                .tail
                .load(Ordering::SeqCst, guard)
                .into_usize(),
            Ordering::SeqCst,
        );

        let mut to_persist = tiny_vec!([usize; MAX_THREADS]);
        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `q` thread has a request that is not yet applied
            if unsafe { self.e_request[q].load(Ordering::SeqCst, guard).deref(pool) }
                .activate
                .load(Ordering::SeqCst)
                != self.e_state[ind].deactivate[q].load(Ordering::SeqCst)
            {
                // 현재 tail의 persist를 예약
                let tail_addr = self.e_state[ind]
                    .tail
                    .load(Ordering::SeqCst, guard)
                    .into_usize();
                match to_persist.binary_search(&tail_addr) {
                    Ok(_) => {} // 같은 주소 중복 persist 방지
                    Err(idx) => to_persist.insert(idx, tail_addr),
                }

                // enq
                let q_req_ref =
                    unsafe { self.e_request[q].load(Ordering::SeqCst, guard).deref(pool) };

                Self::raw_enqueue(&mut self.e_state[ind].tail, q_req_ref.arg, guard, pool);
                E_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                self.e_state[ind].return_val[q] = ();
                self.e_state[ind].deactivate[q]
                    .store(q_req_ref.activate.load(Ordering::SeqCst), Ordering::SeqCst);
            }
        }
        let tail_addr = self.e_state[ind]
            .tail
            .load(Ordering::SeqCst, guard)
            .into_usize();
        match to_persist.binary_search(&tail_addr) {
            Ok(_) => {} // 같은 주소 중복 persist 방지
            Err(idx) => to_persist.insert(idx, tail_addr),
        }
        // persist all in `to_persist`
        while !to_persist.is_empty() {
            let node = PPtr::<Node>::from(to_persist.pop().unwrap());
            persist_obj(unsafe { node.deref(pool) }, false);
        }
        persist_obj(&self.e_request, false);
        persist_obj(&*self.e_state[ind], false);
        sfence();
        self.e_index.store(ind, Ordering::SeqCst);
        persist_obj(&self.e_index, false);
        sfence();
        OLD_TAIL.store(PPtr::<Node>::null().into_offset(), Ordering::SeqCst); // clear old_tail
        drop(lockguard); // release E_LOCK

        // 결과 저장하고 반환
        let res = self.e_state[self.e_index.load(Ordering::SeqCst)].return_val[tid].clone();
        let _ = enq.result.checkpoint::<REC>(res, tid, pool);
        return res;
    }

    /// 실질적인 enqueue: tail 뒤에 새로운 노드 삽입하고 tail로 설정
    fn raw_enqueue(tail: &PAtomic<Node>, arg: Data, guard: &Guard, pool: &PoolHandle) {
        let new_node = POwned::new(
            Node {
                data: arg,
                next: PAtomic::null(),
            },
            pool,
        )
        .into_shared(unsafe { unprotected() });
        let tail_ref = unsafe { tail.load(Ordering::SeqCst, guard).deref_mut(pool) };
        tail_ref.next.store(new_node, Ordering::SeqCst); // tail.next = new node
        tail.store(new_node, Ordering::SeqCst); // tail = new node
    }
}

/// Deq
impl Queue {
    /// deq 요청 등록 후 수행
    pub fn PBQueueDeq<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> DeqRetVal {
        let prev = self.d_request[tid].load(Ordering::SeqCst, guard);
        let prev_activate = if prev.is_null() {
            false
        } else {
            unsafe { prev.deref(pool).activate.load(Ordering::SeqCst) }
        };

        // 새로운 요청 생성
        let req = POwned::new(
            DeqRequestRec {
                activate: AtomicBool::new(!prev_activate),
            },
            pool,
        );
        persist_obj(unsafe { req.deref(pool) }, true);

        let req = ok_or!(
            deq.req.checkpoint::<REC>(PAtomic::from(req), tid, pool),
            e,
            unsafe {
                drop(e.new.load(Ordering::Relaxed, unprotected()).into_owned());
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);

        // 이전에 끝난 client라면 같은 결과 반환
        if REC {
            if let Some(res) = deq.result.peek(tid, pool) {
                return res;
            }
        }

        // 요청 저장소에 등록
        self.d_request[tid].store(req, Ordering::SeqCst);

        // 등록한 요청 수행
        self.PerformDeqReq::<REC>(deq, tid, guard, pool)
    }

    /// 요청 수행
    fn PerformDeqReq<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> DeqRetVal {
        // deq combiner 결정
        let (lval, lockguard) = loop {
            // combiner 되기를 시도. lval을 내가 점유했다면 내가 combiner
            let lval = match D_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret,
                Err((lval, _)) => lval,
            };

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            let backoff = Backoff::new();
            if lval % 2 == 1 {
                while lval == D_LOCK.peek().0 {
                    backoff.snooze();
                }
            }

            if unsafe {
                self.d_request[tid]
                    .load(Ordering::SeqCst, guard)
                    .deref(pool)
            }
            .activate
            .load(Ordering::SeqCst)
                == self.d_state[self.d_index.load(Ordering::SeqCst)].deactivate[tid]
                    .load(Ordering::SeqCst)
            {
                // 자신의 op을 처리한 combiner가 끝날때까지 기다렸다가 결과 반환
                let deactivate_lval = D_DEACTIVATE_LOCK[tid].load(Ordering::SeqCst);
                backoff.reset();
                while !(deactivate_lval < D_LOCK.peek().0) {
                    backoff.snooze();
                }

                // 결과 저장하고 반환
                let res = self.d_state[self.d_index.load(Ordering::SeqCst)].return_val[tid].clone();
                let _ = deq.result.checkpoint::<REC>(res, tid, pool);
                return res;
            }
        };

        // deq combiner는 쌓인 deq 요청들을 수행
        let ind = 1 - self.d_index.load(Ordering::SeqCst);
        self.d_state[ind] = self.d_state[self.d_index.load(Ordering::SeqCst)].clone(); // create a copy of current state

        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `t` thread has a request that is not yet applied
            if unsafe { self.d_request[q].load(Ordering::SeqCst, guard).deref(pool) }
                .activate
                .load(Ordering::SeqCst)
                != self.d_state[self.d_index.load(Ordering::SeqCst)].deactivate[q]
                    .load(Ordering::SeqCst)
            {
                let ret_val;
                // 확실히 persist된 노드들만 deq 수행. OLD_TAIL부터는 현재 enq 중인거라 persist 보장되지 않음
                if OLD_TAIL.load(Ordering::SeqCst)
                    != self.d_state[ind]
                        .head
                        .load(Ordering::SeqCst, guard)
                        .into_usize()
                {
                    ret_val = Self::raw_dequeue(&self.d_state[ind].head, guard, pool);
                } else {
                    ret_val = None;
                }
                let q_req_ref =
                    unsafe { self.d_request[q].load(Ordering::SeqCst, guard).deref(pool) };
                D_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                self.d_state[ind].return_val[q] = ret_val;
                self.d_state[ind].deactivate[q]
                    .store(q_req_ref.activate.load(Ordering::SeqCst), Ordering::SeqCst)
            }
        }
        persist_obj(&self.d_request, false);
        persist_obj(&*self.d_state[ind], false);
        sfence();
        self.d_index.store(ind, Ordering::SeqCst);
        persist_obj(&self.d_index, false);
        sfence();
        drop(lockguard); // release D_LOCK

        // 결과 저장하고 반환
        let res = self.d_state[self.d_index.load(Ordering::SeqCst)].return_val[tid].clone();
        let _ = deq.result.checkpoint::<REC>(res, tid, pool);
        return res;
    }

    /// 실질적인 dequeue: head 한 칸 전진하고 old head를 반환
    fn raw_dequeue(head: &PAtomic<Node>, guard: &Guard, pool: &PoolHandle) -> DeqRetVal {
        let head_shared = head.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head_shared.deref(pool) };

        // NOTE: 원래 구현은 데이터를 반환하질 않고 노드를 반환하므로 free 안함
        let next = head_ref.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            head.store(next, Ordering::SeqCst);
            unsafe { guard.defer_pdestroy(head_shared) };
            return Some(unsafe { next.deref(pool) }.data);
        }
        None
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering;

    use crate::ds::queue_pbcomb::Queue;
    use crate::pmem::{Collectable, GarbageCollection, PoolHandle, RootObj};
    use crate::test_utils::tests::{run_test, TestRootObj, JOB_FINISHED, RESULTS};
    use crossbeam_epoch::Guard;

    use super::{Dequeue, Enqueue};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    struct EnqDeq {
        enqs: [Enqueue; COUNT],
        deqs: [Dequeue; COUNT],
    }

    impl Default for EnqDeq {
        fn default() -> Self {
            Self {
                enqs: array_init::array_init(|_| Enqueue::default()),
                deqs: array_init::array_init(|_| Dequeue::default()),
            }
        }
    }

    impl Collectable for EnqDeq {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..COUNT {
                Enqueue::filter(&mut m.enqs[i], tid, gc, pool);
                Dequeue::filter(&mut m.deqs[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<EnqDeq> for TestRootObj<Queue> {
        fn run(&self, enq_deq: &mut EnqDeq, tid: usize, guard: &Guard, pool: &PoolHandle) {
            // Get &mut queue
            let queue = unsafe { (&self.obj as *const _ as *mut Queue).as_mut() }.unwrap();

            match tid {
                // T1: 다른 스레드들의 실행결과를 확인
                1 => {
                    // 다른 스레드들이 다 끝날때까지 기다림
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check queue is empty
                    let mut tmp_deq = Dequeue::default();
                    let res = queue.PBQueueDeq::<true>(&mut tmp_deq, tid, guard, pool);
                    assert!(res.is_none());

                    // Check results
                    assert!(RESULTS[1].load(Ordering::SeqCst) == 0);
                    assert!((2..NR_THREAD + 2)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
                }
                // T1이 아닌 다른 스레드들은 queue에 { enq; deq; } 수행
                _ => {
                    // enq; deq;
                    for i in 0..COUNT {
                        let val = tid;
                        queue.PBQueueEnq::<true>(val, &mut enq_deq.enqs[i], tid, guard, pool);

                        let res = queue.PBQueueDeq::<true>(&mut enq_deq.deqs[i], tid, guard, pool);
                        assert!(!res.is_none());

                        // deq 결과를 실험결과에 전달
                        let v = res.unwrap();
                        let _ = RESULTS[v].fetch_add(1, Ordering::SeqCst);
                    }

                    // "나 끝났다"
                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "pbcomb_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Queue>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1);
    }
}
