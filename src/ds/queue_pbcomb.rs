//! Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
#![allow(non_snake_case)]
use crate::ds::spin_lock_volatile::VSpinLock;
use crate::pepoch::atomic::Pointer;
use crate::pepoch::{unprotected, PAtomic, POwned};
use crate::ploc::Checkpoint;
use crate::pmem::{persist_obj, sfence, AsPPtr, Collectable, GarbageCollection, PPtr, PoolHandle};
use crate::PDefault;
use array_init::array_init;
use crossbeam_epoch::Guard;
use crossbeam_utils::{Backoff, CachePadded};
use etrace::ok_or;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinyvec::tiny_vec;

const MAX_THREADS: usize = 32;
type Data = usize; // TODO: generic

/// 사용할 스레드 수. combining시 이 스레드 수만큼만 op 순회
pub static mut NR_THREADS: usize = MAX_THREADS;

type EnqRetVal = ();
type DeqRetVal = PPtr<Node>;

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

impl Enqueue {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
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

impl Dequeue {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }
}

#[derive(Debug, Default)]
struct EnqRequestRec {
    arg: usize,
}

impl Collectable for EnqRequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

#[derive(Debug, Default)]
struct DeqRequestRec {}

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
#[derive(Debug, Clone)]
struct EStateRec {
    tail: PAtomic<Node>, // NOTE: reordering 방지를 위한 atomic. CAS는 안씀
    return_val: [Option<EnqRetVal>; MAX_THREADS + 1],
    deactivate: [PAtomic<EnqRequestRec>; MAX_THREADS + 1], // client가 만든 reqeust를 가리킴 (TODO(opt) AtomicUsize로 할까?)
}

impl Collectable for EStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.tail, tid, gc, pool);
        for tid in 0..MAX_THREADS + 1 {
            PAtomic::filter(&mut s.deactivate[tid], tid, gc, pool);
        }
    }
}

/// State of Dequeue PBComb
#[derive(Debug, Clone)]
struct DStateRec {
    head: PAtomic<Node>, // NOTE: reordering 방지를 위한 atomic. CAS는 안씀
    return_val: [Option<DeqRetVal>; MAX_THREADS + 1],
    deactivate: [PAtomic<DeqRequestRec>; MAX_THREADS + 1], // client가 만든 reqeust를 가리킴 (TODO(opt) AtomicUsize로 할까?)
}

impl Collectable for DStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.head, tid, gc, pool);
        for tid in 0..MAX_THREADS + 1 {
            if let Some(mut ret) = s.return_val[tid] {
                PPtr::filter(&mut ret, tid, gc, pool);
            }
            PAtomic::filter(&mut s.deactivate[tid], tid, gc, pool);
        }
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

    /// Used by the PBQueueDEQ instance of PBCOMB
    static ref D_LOCK: VSpinLock = VSpinLock::default();
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
                    return_val: array_init(|_| None),
                    deactivate: array_init(|_| PAtomic::null()),
                })
            }),
            e_index: Default::default(),
            d_request: array_init(|_| Default::default()),
            d_state: array_init(|_| {
                CachePadded::new(DStateRec {
                    head: PAtomic::from(dummy),
                    return_val: array_init(|_| None),
                    deactivate: array_init(|_| PAtomic::null()),
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
        pool: &PoolHandle,
    ) -> EnqRetVal {
        // 새로운 요청 생성
        let req = POwned::new(EnqRequestRec { arg }, pool);
        persist_obj(unsafe { req.deref(pool) }, true);

        let req = ok_or!(
            enq.req.checkpoint::<REC>(PAtomic::from(req), tid, pool),
            e,
            unsafe {
                drop(e.new.load(Ordering::Relaxed, unprotected()).into_owned());
                e.current
            }
        )
        .load(Ordering::Relaxed, unsafe { unprotected() });

        // 이전에 끝난 client라면 같은 결과 반환
        if REC {
            if let Some(res) = enq.result.peek(tid, pool) {
                return res;
            }
        }

        // 요청 저장소에 등록
        self.e_request[tid].store(req, Ordering::SeqCst);

        // 등록한 요청 수행
        self.PerformEnqReq::<REC>(enq, tid, pool)
    }

    /// 요청 수행
    fn PerformEnqReq<const REC: bool>(
        &mut self,
        enq: &mut Enqueue,
        tid: usize,
        pool: &PoolHandle,
    ) -> EnqRetVal {
        // enq combiner 결정
        let lockguard = loop {
            // combiner 되기를 시도. lval을 내가 점유했다면 내가 combiner
            let lval = match E_LOCK.try_lock::<REC>(tid) {
                Ok(g) => break g,
                Err(lval) => lval,
            };

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            let backoff = Backoff::new();
            while lval == E_LOCK.peek() {
                backoff.snooze();
            }

            if self.e_request[tid].load(Ordering::SeqCst, unsafe { unprotected() })
                == self.e_state[self.e_index.load(Ordering::SeqCst)].deactivate[tid]
                    .load(Ordering::SeqCst, unsafe { unprotected() })
            {
                // 자신의 op을 처리한 combiner가 안끝났을 수 있으니, 한 combiner만 더 기다렸다가 결과 반환
                let lval = E_LOCK.peek();
                if lval != 0 {
                    // NOTE: 같은 스레드가 연속적으로 combiner가 되면 starvation 발생가능. 그러나 이 경우는 적을듯
                    backoff.reset();
                    while lval == E_LOCK.peek() {
                        backoff.snooze();
                    }
                }

                // 결과 저장하고 반환
                let res = self.e_state[self.e_index.load(Ordering::SeqCst)].return_val[tid]
                    .clone()
                    .unwrap();
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
                .load(Ordering::SeqCst, unsafe { unprotected() })
                .into_usize(),
            Ordering::SeqCst,
        );

        let mut to_persist = tiny_vec!([usize; MAX_THREADS]);

        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `q` thread has a request that is not yet applied
            if self.e_request[q].load(Ordering::SeqCst, unsafe { unprotected() })
                != self.e_state[ind].deactivate[q].load(Ordering::SeqCst, unsafe { unprotected() })
            {
                // 현재 tail의 persist를 예약
                let tail_addr = self.e_state[ind]
                    .tail
                    .load(Ordering::SeqCst, unsafe { unprotected() })
                    .into_usize();
                match to_persist.binary_search(&tail_addr) {
                    Ok(_) => {} // 같은 주소 중복 persist 방지
                    Err(idx) => to_persist.insert(idx, tail_addr),
                }

                // enq
                let q_req = self.e_request[q].load(Ordering::SeqCst, unsafe { unprotected() });
                Self::raw_enqueue(
                    &mut self.e_state[ind].tail,
                    unsafe { q_req.deref(pool) }.arg,
                    pool,
                );
                self.e_state[ind].return_val[q] = Some(());
                self.e_state[ind].deactivate[q].store(q_req, Ordering::SeqCst);
            }
        }
        let tail_addr = self.e_state[ind]
            .tail
            .load(Ordering::SeqCst, unsafe { unprotected() })
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
        let res = self.e_state[self.e_index.load(Ordering::SeqCst)].return_val[tid]
            .clone()
            .unwrap();
        let _ = enq.result.checkpoint::<REC>(res, tid, pool);
        return res;
    }

    /// 실질적인 enqueue: tail 뒤에 새로운 노드 삽입하고 tail로 설정
    fn raw_enqueue(tail: &PAtomic<Node>, arg: Data, pool: &PoolHandle) {
        let new_node = POwned::new(
            Node {
                data: arg,
                next: PAtomic::null(),
            },
            pool,
        )
        .into_shared(unsafe { unprotected() });
        let tail_ref = unsafe { tail.load(Ordering::SeqCst, unprotected()).deref_mut(pool) };
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
        pool: &PoolHandle,
    ) -> DeqRetVal {
        // 새로운 요청 생성
        let req = POwned::new(DeqRequestRec {}, pool);
        persist_obj(unsafe { req.deref(pool) }, true);

        let req = ok_or!(
            deq.req.checkpoint::<REC>(PAtomic::from(req), tid, pool),
            e,
            unsafe {
                drop(e.new.load(Ordering::Relaxed, unprotected()).into_owned());
                e.current
            }
        )
        .load(Ordering::Relaxed, unsafe { unprotected() });

        // 이전에 끝난 client라면 같은 결과 반환
        if REC {
            if let Some(res) = deq.result.peek(tid, pool) {
                return res;
            }
        }

        // 요청 저장소에 등록
        self.d_request[tid].store(req, Ordering::SeqCst);

        // 등록한 요청 수행
        self.PerformDeqReq::<REC>(deq, tid, pool)
    }

    /// 요청 수행
    fn PerformDeqReq<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        pool: &PoolHandle,
    ) -> DeqRetVal {
        // deq combiner 결정
        let lockguard = loop {
            // combiner 되기를 시도. lval을 내가 점유했다면 내가 combiner
            let lval = match D_LOCK.try_lock::<REC>(tid) {
                Ok(g) => break g,
                Err(lval) => lval,
            };

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            let backoff = Backoff::new();
            while lval == D_LOCK.peek() {
                backoff.snooze();
            }

            if self.d_request[tid].load(Ordering::SeqCst, unsafe { unprotected() })
                == self.d_state[self.d_index.load(Ordering::SeqCst)].deactivate[tid]
                    .load(Ordering::SeqCst, unsafe { unprotected() })
            {
                // 자신의 op을 처리한 combiner가 안끝났을 수 있으니, 한 combiner만 더 기다렸다가 결과 반환
                let lval = D_LOCK.peek();
                if lval != 0 {
                    // NOTE: 같은 스레드가 연속적으로 combiner가 되면 starvation 발생가능. 그러나 이 경우는 적을듯
                    backoff.reset();
                    while lval == D_LOCK.peek() {
                        backoff.snooze();
                    }
                }

                // 결과 저장하고 반환
                let res = self.d_state[self.d_index.load(Ordering::SeqCst)].return_val[tid]
                    .clone()
                    .unwrap();
                let _ = deq.result.checkpoint::<REC>(res, tid, pool);
                return res;
            }
        };

        // deq combiner는 쌓인 deq 요청들을 수행
        let ind = 1 - self.d_index.load(Ordering::SeqCst);
        self.d_state[ind] = self.d_state[self.d_index.load(Ordering::SeqCst)].clone(); // create a copy of current state

        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `t` thread has a request that is not yet applied
            if self.d_request[q].load(Ordering::SeqCst, unsafe { unprotected() })
                != self.d_state[ind].deactivate[q].load(Ordering::SeqCst, unsafe { unprotected() })
            {
                let ret_val;
                // 확실히 persist된 노드들만 deq 수행. OLD_TAIL부터는 현재 enq 중인거라 persist 보장되지 않음
                if OLD_TAIL.load(Ordering::SeqCst)
                    != self.d_state[ind]
                        .head
                        .load(Ordering::SeqCst, unsafe { unprotected() })
                        .into_usize()
                {
                    let node = Self::raw_dequeue(&self.d_state[ind].head, pool);
                    ret_val = Some(node);
                } else {
                    ret_val = Some(PPtr::null());
                }
                self.d_state[ind].return_val[q] = ret_val;
                self.d_state[ind].deactivate[q].store(
                    self.d_request[q].load(Ordering::SeqCst, unsafe { unprotected() }),
                    Ordering::SeqCst,
                )
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
        let res = self.d_state[self.d_index.load(Ordering::SeqCst)].return_val[tid]
            .clone()
            .unwrap();
        let _ = deq.result.checkpoint::<REC>(res, tid, pool);
        return res;
    }

    /// 실질적인 dequeue: head 한 칸 전진하고 old head를 반환
    fn raw_dequeue(head: &PAtomic<Node>, pool: &PoolHandle) -> DeqRetVal {
        let head_ref = unsafe { head.load(Ordering::SeqCst, unprotected()).deref(pool) };

        // NOTE: 얘네 구현은 데이터를 반환하질 않고 노드를 반환.
        // 노드를 dequeue해간 애는 다 썼다고 함부로 노드 free하면 안됨. queue의 sentinel 노드로 남아있을 수 있음. TODO: 그냥 data 반환해도 될 것같은데..왜지
        let ret = head_ref
            .next
            .load(Ordering::SeqCst, unsafe { unprotected() });
        if !ret.is_null() {
            head.store(ret, Ordering::SeqCst);
        }
        PPtr::from(ret.into_usize())
    }

    fn is_empty(&self, guard: &Guard, pool: &PoolHandle) -> bool {
        let head_ref = unsafe {
            self.d_state[self.d_index.load(Ordering::SeqCst)]
                .head
                .load(Ordering::SeqCst, guard)
                .deref(pool)
        };
        let next = head_ref.next.load(Ordering::SeqCst, guard);
        next.is_null()
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

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1000;

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
                    let res = queue.PBQueueDeq::<true>(&mut tmp_deq, tid, pool);
                    assert!(res.is_null());

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
                        queue.PBQueueEnq::<true>(val, &mut enq_deq.enqs[i], tid, pool);

                        let res = queue.PBQueueDeq::<true>(&mut enq_deq.deqs[i], tid, pool);
                        assert!(!res.is_null());

                        // deq 결과를 실험결과에 전달
                        let v = unsafe { res.deref(pool) }.data;
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
