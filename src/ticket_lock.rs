//! Persistent ticket lock

use std::{fmt::Debug, sync::atomic::AtomicUsize};

use crate::persistent::*;

/// TODO: doc
#[derive(Debug)]
pub struct Guard<'l, T> {
    lock: &'l TicketLock<T>
    // TODO: token
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        // TODO: 구현
    }
}

impl<'l, T> Guard<'l, T> {
    /// TODO: doc
    pub fn defer_unlock(guard: Frozen<Guard<'l, T>>) -> Self {
        unsafe { guard.own() }
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct Lock {
    // TODO: 구현
}

impl<'l, T> POp<&'l TicketLock<T>> for Lock {
    type Input = ();
    type Output = Frozen<Guard<'l, T>>;

    /// Guard를 얼려서 반환하므로 unlock을 하기 위해선 Guard::defer_unlock()을 호출해야 함.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let x = TicketLock<usize>::default();
    /// let lock = Lock; // Assume this is on persistent location.
    ///
    /// {
    ///     let guard = lock.run(&x, ());
    ///     let _guard = Guard::defer_unlock(guard);
    ///
    ///     ... // Critical section
    /// } // Unlock when `_guard` is dropped
    /// ```
    // TODO: 구현
    fn run(&mut self, lock: &'l TicketLock<T>, _: Self::Input) -> Self::Output {
        Frozen::from(Guard { lock })
    }

    #[allow(unused_variables)]
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
    use std::{collections::VecDeque, marker::PhantomData};

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    type Queue<T> = VecDeque<T>;
    type LockBasedQueue<T> = TicketLock<Queue<T>>;

    struct PushPop<T> {
        lock: Lock,
        resetting: bool,
        _marker: PhantomData<T> // TODO: T를 위한 임시. 원래는 POp인 Push<T>, Pop<T>가 있어야 함.
    }

    impl<T> Default for PushPop<T> {
        fn default() -> Self {
            Self {
                lock: Default::default(),
                resetting: false,
                _marker: PhantomData
            }
        }
    }

    // TODO: lifetime parameter `'q` only used once this lifetime...
    impl<T: Clone> POp<&LockBasedQueue<T>> for PushPop<T> {
        type Input = T;
        type Output = Option<T>;

        // TODO: 쓰임새를 보이는 용도로 VecDequeue의 push_back(), pop_back()를 사용.
        //       이들은 PersistentOp이 아니므로 이 run()은 지금은 idempotent 하지 않음.
        fn run(&mut self, queue: &LockBasedQueue<T>, input: Self::Input) -> Self::Output {
            if self.resetting {
                self.reset(false);
            }

            // Lock the object
            let guard = self.lock.run(queue, ());
            let _guard = Guard::defer_unlock(guard);

            // Push & Pop
            let q: &mut Queue<T> = unsafe { &mut *(&queue.inner as *const _ as *mut _) };
            q.push_back(input);
            q.pop_front()
        } // Unlock when `_guard` is dropped

        fn reset(&mut self, nested: bool) {
            if !nested {
                self.resetting = true;
            }

            // self.lock.reset(true); // TODO: cannot infer type for type parameter `T` (in POp<T>)
            todo!("reset Push and Pop");

            #[allow(unreachable_code)]
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
