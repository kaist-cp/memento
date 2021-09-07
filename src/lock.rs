//! Persistent lock

use std::{cell::UnsafeCell, marker::PhantomData, ops::{Deref, DerefMut}};

use crate::persistent::*;

pub trait RawLock: Default + Send + Sync {
    type Token: Clone;

    type Lock: POp<&Self, Output = Self::Token>; // TODO: ref & lifetime

    /// # Safety
    ///
    /// `unlock()` should be called with the token given by the corresponding `lock()`.
    unsafe fn unlock(&self, token: Self::Token);
}

#[derive(Debug)]
pub struct LockBased<T, L: RawLock> {
    data: UnsafeCell<T>,
    lock: L,
}

impl<T, L: RawLock> From<T> for LockBased<T, L> {
    fn from(value: T) -> Self {
        Self {
            data: UnsafeCell::new(value),
            lock: L::default(),
        }
    }
}

unsafe impl<T: Send, L: RawLock> Send for LockBased<T, L> {}
unsafe impl<T: Send, L: RawLock> Sync for LockBased<T, L> {}

pub struct Lock<L: RawLock> {
    lock: L::Lock
}

impl<L: RawLock> Default for Lock<L> {
    fn default() -> Self {
        unimplemented!()
    }
}

impl<'l, T, L: RawLock> POp<&'l LockBased<T, L>> for Lock<L> {
    type Input = ();
    type Output = Frozen<LockGuard<'l, T, L>>;

    fn run(&mut self, locked: &LockBased<T, L>, _: Self::Input) -> Self::Output {
        let token = self.lock.run(&locked.lock, ());
        Frozen::from(LockGuard {
            lock: &locked,
            token,
            _marker: Default::default()
        })
    }

    fn reset(&mut self, nested: bool) {
        self.lock.reset(nested);
    }
}

unsafe impl<L: RawLock> Send for Lock<L> {}

/// TODO: doc
#[derive(Debug)]
pub struct LockGuard<'l, T, L: RawLock> {
    lock: &'l LockBased<T, L>,
    token: L::Token,
    _marker: PhantomData<*const ()>, // !Send + !Sync
}

impl<T, L: RawLock> Drop for LockGuard<'_, T, L> {
    fn drop(&mut self) {
        // TODO: 구현
        // curr 증가시키고 op을 reset하자
    }
}

impl<T, L: RawLock> Deref for LockGuard<'_, T, L> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T, L: RawLock> DerefMut for LockGuard<'_, T, L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<'l, T, L: RawLock> LockGuard<'l, T, L> {
    /// TODO: doc
    pub fn defer_unlock(guard: Frozen<LockGuard<'l, T, L>>) -> Self {
        unsafe { guard.own() }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crossbeam_utils::thread;

    use super::*;
    use std::{collections::VecDeque, marker::PhantomData};

    type Queue<T> = VecDeque<T>;

    struct PushPop<T, L: RawLock> {
        lock: Lock<L>,
        resetting: bool,
        _marker: PhantomData<T>, // TODO: T를 위한 임시. 원래는 POp인 Push<T>, Pop<T>가 있어야 함.
    }

    impl<T, L: RawLock> Default for PushPop<T, L> {
        fn default() -> Self {
            Self {
                lock: Default::default(),
                resetting: false,
                _marker: PhantomData,
            }
        }
    }

    impl<T: Clone, L: RawLock> POp<&LockBased<Queue<T>, L>> for PushPop<T, L> {
        type Input = T;
        type Output = Option<T>;

        // TODO: 쓰임새를 보이는 용도로 VecDequeue의 push_back(), pop_back()를 사용.
        //       이들은 PersistentOp이 아니므로 이 run()은 지금은 idempotent 하지 않음.
        fn run(&mut self, queue: &LockBased<Queue<T>, L>, input: Self::Input) -> Self::Output {
            if self.resetting {
                self.reset(false);
            }

            // Lock the object
            let guard = self.lock.run(queue, ());
            let q = LockGuard::defer_unlock(guard);

            // Push & Pop
            q.push_back(input);
            q.pop_front()
        } // Unlock when `q` is dropped

        fn reset(&mut self, nested: bool) {
            if !nested {
                self.resetting = true;
            }

            POp::<&LockBased<Queue<T>, L>>::reset(&mut self.lock, true);
            todo!("reset Push and Pop");

            #[allow(unreachable_code)]
            if !nested {
                self.resetting = false;
            }
        }
    }

    /// Lock-based queue에 push/pop 연산하는 테스트
    pub(crate) fn test_push_pop_queue<L: RawLock>(nr_thread: usize, cnt: usize) {
        let q = LockBased::<Queue<usize>, L>::from(Queue::<_>::default()); // TODO(persistent location)
        let mut push_pops: Vec<Vec<PushPop<usize, L>>> = (0..nr_thread)
            .map(|_| (0..cnt).map(|_| PushPop::default()).collect())
            .collect(); // TODO(persistent location)

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..nr_thread {
                let q = &q;
                let push_pops = unsafe {
                    (push_pops.get_unchecked_mut(tid) as *mut Vec<PushPop<usize, L>>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    for i in 0..cnt {
                        // Check if push_pop acts like an identity function
                        // lock 구현 안 되어 있으므로 assertion 실패함
                        assert_eq!(push_pops[i].run(q, tid), Some(tid));
                    }
                });
            }
        })
        .unwrap();
    }
}
