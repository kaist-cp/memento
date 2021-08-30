//! Persistent ticket lock

use std::{fmt::Debug, marker::PhantomData, sync::atomic::AtomicUsize};

use crate::persistent::POp;

/// TODO: doc
#[derive(Debug)] // 문제: Guard를 Clone으로 해야된다는 건 비직관적
pub struct Guard<'l, T> {
    lock: &'l TicketLock<T>
    // TODO: token
}

impl<T> Clone for Guard<'_, T> {
    fn clone(&self) -> Self {
        Self {
            lock: self.lock.clone()
        }
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        // TODO: 구현
        // TODO: clone 된 guard에 대처
        //       - ticket lock의 curr와 같은지 확인
        //       - overflow wrap되어 한바퀴 돈 ticket을 unlock하는 불상사 대비
    }
}

/// TODO: doc
#[derive(Debug)]
pub struct Lock<T> {
    // TODO: 구현
    _marker: PhantomData<T>
}

impl<T> Default for Lock<T> {
    fn default() -> Self {
        Self {
            _marker: Default::default()
        }
    }
}

// TODO: the lifetime parameter `'l` is not constrained by the impl trait, self type, or predicates unconstrained lifetime parameter
impl<'l, T> POp for Lock<T> {
    type Object = &'l TicketLock<T>;
    type Input = ();
    type Output = Guard<'l, T>;

    fn run(&mut self, lock: Self::Object, _: Self::Input) -> Self::Output {
        // TODO: 구현
        Guard { lock }
    }

    fn reset(&mut self, nested: bool) {
        unimplemented!()
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct TicketLock<T> {
    inner: T,
    curr: AtomicUsize,
    next: AtomicUsize,
}

#[cfg(test)]
mod tests {
    use crossbeam_utils::thread;

    use super::*;
    use std::collections::VecDeque;

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    type Queue<T> = VecDeque<T>;
    type LockBasedQueue<T> = TicketLock<Queue<T>>;

    struct PushPop<T> {
        lock: Lock<LockBasedQueue<T>>,
        // TODO: Queue의 push,
        // TODO: Queue의 Pop,
        resetting: bool
    }

    impl<T> Default for PushPop<T> {
        fn default() -> Self {
            Self {
                lock: Default::default(),
                resetting: false
            }
        }
    }

    // TODO: lifetime parameter `'q` only used once this lifetime...
    impl<'q, T: Clone> POp for PushPop<T> {
        type Object = &'q LockBasedQueue<T>;
        type Input = T;
        type Output = Option<T>;

        // TODO: 쓰임새를 보이는 용도로 VecDequeue의 push_back(), pop_back()를 사용.
        //       이들은 PersistentOp이 아니므로 이 run()은 지금은 idempotent 하지 않음.
        fn run(&mut self, queue: Self::Object, input: Self::Input) -> Self::Output {
            if self.resetting {
                self.reset(false);
            }

            // Lock the object
            let _guard = self.lock.run(queue, ());

            // Push & Pop
            let q: &mut Queue<T> = unsafe { &mut *(&queue.inner as *const _ as *mut _) };
            q.push_back(input);
            q.pop_front()
        } // Unlock when `_guard` is dropped

        fn reset(&mut self, nested: bool) {
            if !nested {
                self.resetting = true;
            }

            self.lock.reset(true);
            todo!("reset Push and Pop");

            if !nested {
                self.resetting = false;
            }
        }
    }

    #[test]
    fn push_pop_seq_queue() {
        let obj = LockBasedQueue::default(); // TODO(persistent location)
        let mut push_pops: Vec<Vec<PushPop<usize>>> = (0..NR_THREAD)
            .map(|_| (0..COUNT).map(|_| PushPop::default()).collect())
            .collect(); // TODO(persistent location)

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..NR_THREAD {
                let obj = &obj;
                let push_pops = unsafe {
                    (push_pops.get_unchecked_mut(tid) as *mut Vec<PushPop<usize>>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    for i in 0..COUNT {
                        // Check if push_pop acts like an identity function
                        // lock 구현 안 되어 있으므로 assertion 실패함
                        assert_eq!(push_pops[i].run(obj, tid), Some(tid));
                    }
                });
            }
        })
        .unwrap();
    }
}
