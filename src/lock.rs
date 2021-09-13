//! Persistent lock

use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::persistent::*;

/// Persistent raw lock
pub trait RawLock: Default + Send + Sync {
    /// lock 잡았음을 증명하는 토큰
    type Token: Clone;

    /// Lock operation을 수행하는 POp
    type Lock<'l>: POp<&'l Self, Input = (), Output = Self::Token>
    where
        Self: 'l;

    /// Unlock operation을 수행하는 POp
    type Unlock<'l>: POp<&'l Self, Input = Self::Token, Output = ()>
    where
        Self: 'l;
}

/// TODO: doc
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

/// LockGuard를 얼려서 반환하므로 사용하기 위해선 Guard::defer_unlock()을 호출해야 함.
///
/// # Examples
///
/// ```rust
/// // Assume these are on persistent location:
/// let x = LockBased<i32, TicketLock>::default();
/// let lock = Lock;
///
/// {
///     let guard = lock.run(&x, ());
///     let v = unsafe { Guard::defer_unlock(guard) };
///
///     ... // Critical section
/// } // Unlock when `v` is dropped
/// ```
#[derive(Debug)]
pub struct Lock<'l, L: 'l + RawLock> {
    lock: L::Lock<'l>,
    unlock: L::Unlock<'l>,
}

impl<'l, L: 'l + RawLock> Default for Lock<'l, L> {
    fn default() -> Self {
        unimplemented!()
    }
}

impl<'l, T, L: RawLock> POp<&'l LockBased<T, L>> for Lock<'l, L> {
    type Input = ();
    type Output = Frozen<LockGuard<'l, T, L>>;

    fn run(&mut self, locked: &'l LockBased<T, L>, _: Self::Input) -> Self::Output {
        let token = self.lock.run(&locked.lock, ());
        Frozen::from(LockGuard {
            locked,
            unlock: &mut unsafe { *(&mut self.unlock as *mut _) }, // TODO: How to safely borrow
            token,
            _marker: Default::default(),
        })
    }

    fn reset(&mut self, nested: bool) {
        self.lock.reset(nested);
    }
}

unsafe impl<'l, L: 'l + RawLock> Send for Lock<'l, L> {}

/// TODO: doc
#[derive(Debug)]
pub struct LockGuard<'l, T, L: RawLock> {
    locked: &'l LockBased<T, L>,
    unlock: &'l mut L::Unlock<'l>,
    token: L::Token,
    _marker: PhantomData<*const ()>, // !Send + !Sync
}

impl<T, L: RawLock> Drop for LockGuard<'_, T, L> {
    fn drop(&mut self) {
        self.unlock.run(&self.locked.lock, self.token.clone());
    }
}

impl<T, L: RawLock> Deref for LockGuard<'_, T, L> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.locked.data.get() }
    }
}

impl<T, L: RawLock> DerefMut for LockGuard<'_, T, L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.locked.data.get() }
    }
}

impl<'l, T, L: RawLock> LockGuard<'l, T, L> {
    /// 보호된 데이터의 접근 권한을 얻고 unlock을 예약함.
    ///
    /// # Safety
    ///
    /// `LockBased`에 대한 `POp`들은 `Lock`보다 나중에 reset 되어야 함.
    /// 이유: 그렇지 않으면, 서로 다른 스레드가 `LockGuard`를 각각 가지고 있을 때 모두 fresh `POp`을 수행할 수 있으므로 mutex가 깨짐.
    pub unsafe fn defer_unlock(guard: Frozen<LockGuard<'l, T, L>>) -> Self {
        guard.own()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crossbeam_utils::thread;

    use super::*;
    use std::{collections::VecDeque, marker::PhantomData};

    type Queue<T> = VecDeque<T>;

    struct PushPop<'l, T, L: RawLock> {
        lock: Lock<'l, L>,
        resetting: bool,
        _marker: PhantomData<T>, // TODO: T를 위한 임시. 원래는 POp인 Push<T>, Pop<T>가 있어야 함.
    }

    impl<T, L: RawLock> Default for PushPop<'_, T, L> {
        fn default() -> Self {
            Self {
                lock: Default::default(),
                resetting: false,
                _marker: PhantomData,
            }
        }
    }

    impl<'l, T: 'l + Clone, L: RawLock> POp<&'l LockBased<Queue<T>, L>> for PushPop<'l, T, L> {
        type Input = T;
        type Output = Option<T>;

        // TODO: 쓰임새를 보이는 용도로 VecDequeue의 push_back(), pop_back()를 사용.
        //       이들은 PersistentOp이 아니므로 이 run()은 지금은 idempotent 하지 않음.
        fn run(&mut self, queue: &'l LockBased<Queue<T>, L>, input: Self::Input) -> Self::Output {
            if self.resetting {
                self.reset(false);
            }

            // Lock the object
            let guard = self.lock.run(queue, ());
            let mut q = unsafe { LockGuard::defer_unlock(guard) };

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
        let mut push_pops: Vec<Vec<PushPop<'_, usize, L>>> = (0..nr_thread)
            .map(|_| (0..cnt).map(|_| PushPop::default()).collect())
            .collect(); // TODO(persistent location)

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..nr_thread {
                let q = &q;
                let push_pops = unsafe {
                    (push_pops.get_unchecked_mut(tid) as *mut Vec<PushPop<'_, usize, L>>)
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
