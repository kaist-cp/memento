//! Detectable Combining queue
#![allow(non_snake_case)]
#![allow(warnings)]
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
    data: Data,
    next: PAtomic<Node>,
}

impl Collectable for Node {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.next, tid, gc, pool);
    }
}

/// TODO: doc
pub trait Combinable {
    /// TODO: doc
    fn checkpoint_activate(&self, activate: usize) -> usize;

    /// TODO: doc
    fn checkpoint_return_value(&self, return_value: usize) -> usize;
}

/// TODO: doc
#[derive(Debug)]
pub struct CombStateRec {
    state: *mut c_void, // The actual data of the state e.g. tail for enqueue, head for dequeue
    return_value: [usize; MAX_THREADS],
    deactivate: [AtomicUsize; MAX_THREADS],
    // TODO: flex?
}

impl Clone for CombStateRec {
    fn clone(&self) -> Self {
        todo!()
    }
}

/// TODO: doc
#[derive(Debug)]
pub struct CombRequest {
    arg: AtomicUsize,
    operation: u64,
    activate: AtomicUsize,
}

/// TODO: doc
#[allow(missing_debug_implementations)]
pub struct CombStruct {
    // General func for additional behavior: e.g. persist enqueued nodes
    final_persist_func: Option<&'static dyn Fn(&CombStruct)>,
    after_persist_func: Option<&'static dyn Fn(&CombStruct)>,

    // Variables located at volatile location
    lock: &'static CachePadded<ThreadRecoverableSpinLock>,
    lock_value: &'static CachePadded<AtomicUsize>,

    // Variables located at persistent location
    request: [CachePadded<CombRequest>; MAX_THREADS], // TODO: pointer?
    pstate: CachePadded<PAtomic<CombStateRec>>,       // TODO: PAtomic<CombStateRec>?
}

/// TODO: doc
#[derive(Debug)]
pub struct CombThreadState {
    index: AtomicUsize,
    state: [PAtomic<CombStateRec>; 2],
}

/// TODO: doc
#[derive(Debug)]
pub struct Combining {}

impl Combining {
    /// TODO: doc
    // TODO: generalize return value
    pub fn apply_op<const REC: bool, M: Combinable>(
        &self,
        mmt: &M,
        s: &CombStruct,
        st_thread: &CombThreadState,
        sfunc: &dyn Fn(*mut c_void, usize, usize) -> usize,
        arg: usize,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        // Register request
        s.request[tid].arg.store(arg, Ordering::SeqCst);
        s.request[tid].activate.store(
            mmt.checkpoint_activate(s.request[tid].activate.load(Ordering::SeqCst) + 1),
            Ordering::SeqCst,
        );

        // Do
        loop {
            match s.lock.try_lock::<REC>(tid) {
                Ok((lval, lockguard)) => {
                    return self
                        .do_combine(lval, lockguard, mmt, s, st_thread, sfunc, tid, guard, pool)
                }
                Err((lval, _)) => {
                    if let Ok(retval) = self.do_non_combine(lval, mmt, s, tid, guard, pool) {
                        return retval;
                    }
                }
            }
        }
    }

    fn do_combine<M: Combinable>(
        &self,
        lval: usize,
        lockguard: SpinLockGuard<'_>,
        mmt: &M,
        s: &CombStruct,
        st_thread: &CombThreadState,
        sfunc: &dyn Fn(*mut c_void, usize, usize) -> usize, // (state, arg, tid) -> return value
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
                        new_state_ref.state,
                        s.request[t].arg.load(Ordering::SeqCst),
                        tid,
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
            func(s);
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
            func(s);
        }

        st_thread.index.store(1 - ind, Ordering::SeqCst);

        // checkpoint return value of mmt-local request
        let ret_val = mmt.checkpoint_return_value(new_state_ref.return_value[tid].clone());
        drop(lockguard);
        return ret_val;
    }

    fn do_non_combine<M: Combinable>(
        &self,
        lval: usize,
        mmt: &M,
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
                return Ok(mmt.checkpoint_return_value(lastest_state.return_value[tid].clone()));
            }

            // wait until the combiner that processed my op is finished
            backoff.reset();
            while s.lock.peek().0 == lval + 2 {
                backoff.snooze();
            }
            // checkpoint return value of mmt-local request
            return Ok(mmt.checkpoint_return_value(lastest_state.return_value[tid].clone()));
        }

        Err(())
    }
}
