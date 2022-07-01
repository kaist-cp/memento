//! Detectable Combining queue
#![allow(non_snake_case)]
#![allow(warnings)]
#![allow(missing_docs)]
use crate::ds::tlock::ThreadRecoverableSpinLock;
use crate::pepoch::atomic::Pointer;
use crate::pepoch::{unprotected, PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::Checkpoint;
use crate::pmem::{
    global_pool, persist_obj, sfence, Collectable, GarbageCollection, PPtr, PoolHandle,
};
use crate::PDefault;
use array_init::array_init;
use crossbeam_epoch::Guard;
use crossbeam_utils::{Backoff, CachePadded};
use etrace::ok_or;
use libc::c_void;
use std::error::Error;
use std::sync::atomic::{fence, AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use tinyvec::tiny_vec;

use super::tlock::SpinLockGuard;

const MAX_THREADS: usize = 64;
type Data = usize;

const COMBINING_ROUNDS: usize = 20;

/// restriction of combining iteration
pub static mut NR_THREADS: usize = MAX_THREADS;

/// Node
#[derive(Debug)]
#[repr(align(128))]
pub struct Node {
    pub data: Data,
    pub next: PAtomic<Node>,
}

impl Collectable for Node {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.next, tid, gc, pool);
    }
}

/// TODO: doc
pub trait Combinable {
    /// TODO: doc
    fn checkpoint_activate<const REC: bool>(
        &mut self,
        activate: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> usize;

    /// TODO: doc
    fn checkpoint_return_value<const REC: bool>(
        &mut self,
        return_value: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> usize;
}

/// TODO: doc
#[derive(Debug)]
pub struct CombStateRec {
    pub data: PAtomic<c_void>, // The actual data of the state e.g. tail for enqueue, head for dequeue
    return_value: [usize; MAX_THREADS + 1],
    deactivate: [AtomicUsize; MAX_THREADS + 1],
    // TODO: flex?
}

impl CombStateRec {
    pub fn new<T>(data: PAtomic<T>) -> Self {
        // let a = unsafe { (&data as *const _ as *const PAtomic<c_void>).read() }
        Self {
            data: unsafe { (&data as *const _ as *const PAtomic<c_void>).read() },
            return_value: array_init(|_| Default::default()),
            deactivate: array_init(|_| Default::default()),
        }
    }
}

impl Clone for CombStateRec {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            return_value: array_init(|i| self.return_value[i]),
            deactivate: array_init(|i| AtomicUsize::new(self.deactivate[i].load(Ordering::SeqCst))),
        }
    }
}

/// TODO: doc
#[derive(Default, Debug)]
pub struct CombRequest {
    arg: AtomicUsize,
    activate: AtomicUsize,
}

/// TODO: doc
#[allow(missing_debug_implementations)]
pub struct CombStruct {
    // General func for additional behavior: e.g. persist enqueued nodes
    final_persist_func: Option<&'static dyn Fn(&CombStruct, &Guard, &PoolHandle)>,
    after_persist_func: Option<&'static dyn Fn(&CombStruct, &Guard, &PoolHandle)>,

    // Variables located at volatile location
    lock: &'static CachePadded<ThreadRecoverableSpinLock>,
    lock_value: &'static CachePadded<AtomicUsize>,

    // Variables located at persistent location
    request: [CachePadded<CombRequest>; MAX_THREADS + 1], // TODO: pointer?
    pub pstate: CachePadded<PAtomic<CombStateRec>>,       // TODO: PAtomic<CombStateRec>?
}

impl CombStruct {
    pub fn new(
        final_persist_func: Option<&'static dyn Fn(&CombStruct, &Guard, &PoolHandle)>,
        after_persist_func: Option<&'static dyn Fn(&CombStruct, &Guard, &PoolHandle)>,
        lock: &'static CachePadded<ThreadRecoverableSpinLock>,
        lock_value: &'static CachePadded<AtomicUsize>,
        request: [CachePadded<CombRequest>; MAX_THREADS + 1],
        pstate: CachePadded<PAtomic<CombStateRec>>,
    ) -> Self {
        Self {
            final_persist_func,
            after_persist_func,
            lock,
            lock_value,
            request,
            pstate,
        }
    }
}

/// TODO: doc
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

/// TODO: doc
#[derive(Debug)]
pub struct Combining {}

impl Combining {
    /// TODO: doc
    // TODO: generalize return value
    // TODO: retval option?
    pub fn apply_op<const REC: bool, M: Combinable>(
        // &self,
        mmt: &mut M,
        s: &CombStruct,
        st_thread: &CombThreadState,
        sfunc: &dyn Fn(&PAtomic<c_void>, usize, usize, &Guard, &PoolHandle) -> usize,
        arg: usize, // TODO: option?
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        // Register request
        s.request[tid].arg.store(arg, Ordering::SeqCst);
        s.request[tid].activate.store(
            mmt.checkpoint_activate::<REC>(
                s.request[tid].activate.load(Ordering::SeqCst) + 1,
                tid,
                pool,
            ),
            Ordering::SeqCst,
        );

        // Do
        loop {
            match s.lock.try_lock::<REC>(tid) {
                Ok((lval, lockguard)) => {
                    return Self::do_combine::<REC, _>(
                        lval, lockguard, mmt, s, st_thread, sfunc, tid, guard, pool,
                    )
                }
                Err((lval, _)) => {
                    if let Ok(retval) =
                        Self::do_non_combine::<REC, _>(lval, mmt, s, tid, guard, pool)
                    {
                        return retval;
                    }
                }
            }
        }
    }

    fn do_combine<const REC: bool, M: Combinable>(
        // &self,
        lval: usize,
        lockguard: SpinLockGuard<'_>,
        mmt: &mut M,
        s: &CombStruct,
        st_thread: &CombThreadState,
        sfunc: &dyn Fn(&PAtomic<c_void>, usize, usize, &Guard, &PoolHandle) -> usize, // (state, arg, tid) -> return value
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        // deq combiner executes the deq requests
        let ind = st_thread.index.load(Ordering::SeqCst);
        let mut new_state = st_thread.state[ind].load(Ordering::SeqCst, guard);
        let new_state_ref = unsafe { new_state.deref_mut(pool) };
        *new_state_ref = unsafe { s.pstate.load(Ordering::SeqCst, guard).deref(pool) }.clone(); // create a copy of current state

        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

            for t in 1..unsafe { NR_THREADS } + 1 {
                // if `t` thread has a request that is not yet applied
                let t_activate = s.request[t].activate.load(Ordering::SeqCst);
                if t_activate > new_state_ref.deactivate[t].load(Ordering::SeqCst) {
                    new_state_ref.return_value[t] = sfunc(
                        &new_state_ref.data,
                        s.request[t].arg.load(Ordering::SeqCst),
                        tid,
                        guard,
                        pool,
                    );
                    new_state_ref.deactivate[t].store(t_activate, Ordering::SeqCst);

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

        // Persist new state
        persist_obj(new_state_ref, false);
        sfence();

        // Update latest state as new state
        s.lock_value.store(lval, Ordering::SeqCst);
        s.pstate.store(new_state, Ordering::SeqCst);
        persist_obj(&*s.pstate, false);
        sfence();

        // e.g. enqueue: update old tail
        if let Some(func) = s.after_persist_func {
            func(s, guard, pool);
        }

        st_thread.index.store(1 - ind, Ordering::SeqCst);

        // checkpoint return value of mmt-local request
        let ret_val =
            mmt.checkpoint_return_value::<REC>(new_state_ref.return_value[tid].clone(), tid, pool);
        drop(lockguard);
        return ret_val;
    }

    fn do_non_combine<const REC: bool, M: Combinable>(
        // &self,
        lval: usize,
        mmt: &mut M,
        s: &CombStruct,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<usize, ()> {
        // non-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
        let backoff = Backoff::new();
        while lval == s.lock.peek().0 {
            backoff.snooze();
        }

        // checkpoint deactivate of mmt-local request
        let lastest_state = unsafe { s.pstate.load(Ordering::SeqCst, guard).deref(pool) };
        // let lastest_state = unsafe { self.d_state.load(Ordering::SeqCst, guard).deref(pool) };
        if s.request[tid].activate.load(Ordering::SeqCst)
            <= lastest_state.deactivate[tid].load(Ordering::SeqCst)
        {
            if s.lock_value.load(Ordering::SeqCst) == lval {
                // checkpoint return value of mmt-local request
                return Ok(mmt.checkpoint_return_value::<REC>(
                    lastest_state.return_value[tid].clone(),
                    tid,
                    pool,
                ));
            }

            // wait until the combiner that processed my op is finished
            backoff.reset();
            while s.lock.peek().0 == lval + 2 {
                backoff.snooze();
            }
            // checkpoint return value of mmt-local request
            return Ok(mmt.checkpoint_return_value::<REC>(
                lastest_state.return_value[tid].clone(),
                tid,
                pool,
            ));
        }

        Err(())
    }
}
