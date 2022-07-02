//! Detectable Combining queue
#![allow(missing_docs)]
pub mod queue_comb;
use crate::pepoch::PAtomic;
use crate::pmem::{persist_obj, Collectable, GarbageCollection, PoolHandle};
use array_init::array_init;
use crossbeam_epoch::Guard;
use crossbeam_utils::{Backoff, CachePadded};
use libc::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};

use self::combining_lock::CombiningLock;

const MAX_THREADS: usize = 64;
const COMBINING_ROUNDS: usize = 20;

/// restriction of combining iteration
pub static mut NR_THREADS: usize = MAX_THREADS;

/// Node
#[derive(Debug)]
#[repr(align(128))]
pub struct Node {
    pub data: usize,
    pub next: PAtomic<Node>,
}

impl Collectable for Node {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.next, tid, gc, pool);
    }
}

/// Trait for Memento
pub trait Combinable {
    fn checkpoint_activate<const REC: bool>(
        &mut self,
        activate: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> usize;

    fn checkpoint_return_value<const REC: bool>(
        &mut self,
        return_value: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> usize;
}

/// request obj
#[derive(Default, Debug)]
pub struct CombRequest {
    arg: AtomicUsize,
    activate: AtomicUsize,
}

impl Collectable for CombRequest {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

/// state obj
#[derive(Debug)]
pub struct CombStateRec {
    pub data: PAtomic<c_void>, // The actual data of the state (e.g. tail for enqueue, head for dequeue)
    return_value: [usize; MAX_THREADS + 1],
    deactivate: [AtomicUsize; MAX_THREADS + 1],
}

impl CombStateRec {
    pub fn new<T>(data: PAtomic<T>) -> Self {
        Self {
            data: unsafe { (&data as *const _ as *const PAtomic<c_void>).read() },
            return_value: array_init(|_| Default::default()),
            deactivate: array_init(|_| Default::default()),
        }
    }
}

impl Collectable for CombStateRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        // Collectable::filter(&mut s.data, tid, gc, pool); // TODO: void 어케 마크하지? c_void 대신 T로?
    }
}

impl Clone for CombStateRec {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            return_value: array_init(|i| self.return_value[i]),
            deactivate: array_init(|i| {
                AtomicUsize::new(self.deactivate[i].load(Ordering::Relaxed))
            }),
        }
    }
}

/// per-thread state for combining
#[derive(Debug)]
pub struct CombThreadState {
    index: AtomicUsize,
    state: [PAtomic<CombStateRec>; 2],
}

impl CombThreadState {
    pub fn new<T>(data: PAtomic<T>, pool: &PoolHandle) -> Self {
        Self {
            index: Default::default(),
            state: array_init(|_| PAtomic::new(CombStateRec::new(data.clone()), pool)),
        }
    }
}

impl Collectable for CombThreadState {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut s.state[0], tid, gc, pool);
        Collectable::filter(&mut s.state[1], tid, gc, pool);
    }
}

/// Central object for combining
#[allow(missing_debug_implementations)]
pub struct CombStruct {
    // General func for additional behavior: e.g. persist enqueued nodes
    final_persist_func: Option<&'static dyn Fn(&CombStruct, &Guard, &PoolHandle)>,
    after_persist_func: Option<&'static dyn Fn(&CombStruct, &Guard, &PoolHandle)>,

    // Variables located at volatile location
    lock: &'static CachePadded<CombiningLock>,

    // Variables located at persistent location
    request: [CachePadded<CombRequest>; MAX_THREADS + 1], // per-thread requests
    pub pstate: CachePadded<PAtomic<CombStateRec>>,       // stable state
}

impl Collectable for CombStruct {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        for t in 0..s.request.len() {
            Collectable::filter(&mut *s.request[t], tid, gc, pool);
        }
        Collectable::filter(&mut *s.pstate, tid, gc, pool);
    }
}

impl CombStruct {
    pub fn new(
        final_persist_func: Option<&'static dyn Fn(&CombStruct, &Guard, &PoolHandle)>,
        after_persist_func: Option<&'static dyn Fn(&CombStruct, &Guard, &PoolHandle)>,
        lock: &'static CachePadded<CombiningLock>,
        request: [CachePadded<CombRequest>; MAX_THREADS + 1],
        pstate: CachePadded<PAtomic<CombStateRec>>,
    ) -> Self {
        Self {
            final_persist_func,
            after_persist_func,
            lock,
            request,
            pstate,
        }
    }
}

#[derive(Debug)]
pub struct Combining {}

impl Combining {
    // sfunc: (state data (head or tail), arg, tid, guard, pool) -> return value
    pub fn apply_op<const REC: bool, M: Combinable>(
        arg: usize,
        (s, st_thread, sfunc): (
            &CombStruct,
            &CombThreadState,
            &dyn Fn(&PAtomic<c_void>, usize, usize, &Guard, &PoolHandle) -> usize,
        ),
        mmt: &mut M,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        // Register request
        s.request[tid].arg.store(arg, Ordering::Relaxed);
        s.request[tid].activate.store(
            mmt.checkpoint_activate::<REC>(
                s.request[tid].activate.load(Ordering::Relaxed) + 1,
                tid,
                pool,
            ),
            Ordering::Release,
        );

        // Do
        loop {
            match s.lock.try_lock::<REC>(tid) {
                Ok(_) => {
                    return Self::do_combine::<REC, _>((s, st_thread, sfunc), mmt, tid, guard, pool)
                }
                Err(_) => {
                    if let Ok(retval) = Self::do_non_combine::<REC, _>(s, mmt, tid) {
                        return retval;
                    }
                }
            }
        }
    }

    /// combiner가 되면 per-thread(pt) state로 홀짝놀이하며 reqeust를 처리.
    ///
    /// 1. 준비: central obj에 있는 최신 state를 자신의 pt.state[pt.index]로 복사.
    /// 2. 처리: request를 처리하며 최신 상태가 된 pt.state[pt.index]를 업데이트 해나감
    /// 3. 마무리:
    ///     3.1. pt.state[pt.index]를 central obj의 최신 state로 박아넣음 (commit point)
    ///     3.2. pt.index = 1 - pt.index
    ///     3.3. release lock
    fn do_combine<const REC: bool, M: Combinable>(
        (s, st_thread, sfunc): (
            &CombStruct,
            &CombThreadState,
            &dyn Fn(&PAtomic<c_void>, usize, usize, &Guard, &PoolHandle) -> usize,
        ),
        mmt: &mut M,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        // 1. 준비
        let ind = st_thread.index.load(Ordering::Relaxed);
        let mut new_state = st_thread.state[ind].load(Ordering::Relaxed, guard);
        let new_state_ref = unsafe { new_state.deref_mut(pool) };
        *new_state_ref = unsafe { s.pstate.load(Ordering::Relaxed, guard).deref(pool) }.clone(); // create a copy of current state

        // 2. 처리
        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

            for t in 1..unsafe { NR_THREADS } + 1 {
                let t_activate = s.request[t].activate.load(Ordering::Acquire);
                if t_activate > new_state_ref.deactivate[t].load(Ordering::Relaxed) {
                    new_state_ref.return_value[t] = sfunc(
                        &new_state_ref.data,
                        s.request[t].arg.load(Ordering::Relaxed),
                        tid,
                        guard,
                        pool,
                    );
                    new_state_ref.deactivate[t].store(t_activate, Ordering::Release);

                    // cnt
                    serve_reqs += 1;
                }
            }

            if serve_reqs == 0 {
                break;
            }
        }

        // e.g. enqueue: persist all enqueued node
        if let Some(func) = s.final_persist_func {
            func(s, guard, pool);
        }
        persist_obj(new_state_ref, true);

        // 3.1 업데이트한 per-thread state를 global에 최신 state로서 박아넣음
        s.pstate.store(new_state, Ordering::Release);
        persist_obj(&*s.pstate, true);

        // e.g. enqueue: update old tail
        if let Some(func) = s.after_persist_func {
            func(s, guard, pool);
        }

        // 3.2. per-thread index 뒤집기
        st_thread.index.store(1 - ind, Ordering::Relaxed);

        // 3.3. release lock with new state
        unsafe { s.lock.unlock(new_state_ref as *const _ as usize) };

        mmt.checkpoint_return_value::<REC>(new_state_ref.return_value[tid], tid, pool)
    }

    /// non-combiner는 combiner가 끝나기를 기다렸다가 자신의 request가 처리됐는지 확인하고 반환
    fn do_non_combine<const REC: bool, M: Combinable>(
        // &self,
        s: &CombStruct,
        mmt: &mut M,
        tid: usize,
    ) -> Result<usize, ()> {
        // wait until the combiner unlocks the lock
        let backoff = Backoff::new();
        let mut combined_ptr;
        let mut combined_tid;
        loop {
            (combined_ptr, combined_tid) = s.lock.peek();
            if combined_tid == 0 {
                break;
            }
            backoff.snooze();
        }

        // 자신의 request가 처리됐는지 확인
        let lastest_state = unsafe { (combined_ptr as *const CombStateRec).as_ref().unwrap() };
        if s.request[tid].activate.load(Ordering::Relaxed)
            <= lastest_state.deactivate[tid].load(Ordering::Acquire)
        {

        Err(())
    }
}

mod combining_lock {
    //! Thread-recoverable lock for combining
    use core::sync::atomic::Ordering;
    use std::sync::atomic::AtomicUsize;

    use crossbeam_utils::Backoff;

    use crate::impl_left_bits;

    // Auxiliary Bits
    // aux bits: MSB 55-bit in 64-bit
    // Used for:
    // - Comb: Indicating ptr of combined state
    pub(crate) const POS_AUX_BITS: u32 = 0;
    pub(crate) const NR_AUX_BITS: u32 = 55;
    impl_left_bits!(aux_bits, POS_AUX_BITS, NR_AUX_BITS, usize);

    #[inline]
    fn compose_aux_bit(aux: usize, data: usize) -> usize {
        (aux_bits() & (aux.rotate_right(POS_AUX_BITS + NR_AUX_BITS))) | (!aux_bits() & data)
    }

    #[inline]
    fn decompose_aux_bit(data: usize) -> (usize, usize) {
        (
            (data & aux_bits()).rotate_left(POS_AUX_BITS + NR_AUX_BITS),
            !aux_bits() & data,
        )
    }

    /// thread-recoverable spin lock
    #[derive(Debug, Default)]
    pub struct CombiningLock {
        inner: AtomicUsize, // 55:ptr of state, 9:tid occupying the lock
    }

    impl CombiningLock {
        const PTR_NULL: usize = 0;
        const RELEASED: usize = 0;

        /// Try lock
        ///
        /// return Ok: (seq, guard)
        /// return Err: (seq, tid)
        pub fn try_lock<const REC: bool>(&self, tid: usize) -> Result<(), (usize, usize)> {
            let current = self.inner.load(Ordering::Relaxed);
            let (_ptr, _tid) = decompose_aux_bit(current);

            if REC && tid == _tid {
                return Ok(());
            }

            if _tid != Self::RELEASED {
                return Err((_ptr, _tid));
            }

            self.inner
                .compare_exchange(
                    current,
                    compose_aux_bit(Self::PTR_NULL, tid),
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .map(|_| ())
                .map_err(|_| (_ptr, _tid))
        }

        /// lock
        pub fn lock<const REC: bool>(&self, tid: usize) -> () {
            let backoff = Backoff::new();
            loop {
                if let Ok(ret) = self.try_lock::<REC>(tid) {
                    return ret;
                }
                backoff.snooze();
            }
        }

        /// peek
        ///
        /// return (ptr, tid)
        pub fn peek(&self) -> (usize, usize) {
            decompose_aux_bit(self.inner.load(Ordering::Acquire))
        }

        /// unlock
        ///
        /// unlock the lock with given ptr
        pub unsafe fn unlock(&self, ptr: usize) {
            self.inner
                .store(compose_aux_bit(ptr, Self::RELEASED), Ordering::Release);
        }
    }
}
