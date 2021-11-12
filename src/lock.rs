//! Persistent mutex

use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    os::raw::c_char,
};

use crate::{
    persistent::*,
    plocation::{
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// Persistent raw lock
pub trait RawLock: Default + Send + Sync {
    /// lock 잡았음을 증명하는 토큰
    type Token: Clone;

    /// Lock operation을 수행하는 POp
    type Lock: for<'l> POp<Object<'l> = &'l Self, Input = (), Output<'l> = Self::Token, Error = !>;

    /// Unlock operation을 수행하는 POp
    ///
    /// 실제 token이 아닌 값으로 unlock 호출시 panic
    // TODO: Output에 Frozen을 강제해야 할 수도 있음. MutexGuard 인터페이스 없이 RawLock만으로는 critical section의 mutex 보장 못함.
    type Unlock: for<'l> POp<Object<'l> = &'l Self, Input = Self::Token, Output<'l> = (), Error = !>;
}

/// TODO: doc
#[derive(Debug)]
pub struct Mutex<L: RawLock, T> {
    data: UnsafeCell<T>,
    lock: L,
}

impl<L: RawLock, T> From<T> for Mutex<L, T> {
    fn from(value: T) -> Self {
        Self {
            data: UnsafeCell::new(value),
            lock: L::default(),
        }
    }
}

unsafe impl<T: Send, L: RawLock> Send for Mutex<L, T> {}
unsafe impl<T: Send, L: RawLock> Sync for Mutex<L, T> {}

/// MutexGuard를 얼려서 반환하므로 사용하기 위해선 Guard::defer_unlock()을 호출해야 함.
///
/// # Examples
///
/// ```rust
/// // Assume these are on persistent location:
/// let x = Mutex<i32, TicketLock>::default();
/// let lock = Lock;
///
/// {
///     let guard = lock.run(&x, ());
///     let v = unsafe { MutexGuard::defer_unlock(guard) };
///
///     ... // Critical section
/// } // Unlock when `v` is dropped
/// ```
#[derive(Debug)]
pub struct Lock<L: RawLock, T> {
    lock: L::Lock,
    unlock: L::Unlock,
    _marker: PhantomData<*const T>,
}

impl<L: RawLock, T> Default for Lock<L, T> {
    fn default() -> Self {
        unimplemented!()
    }
}

impl<L: RawLock, T> Collectable for Lock<L, T> {
    unsafe extern "C" fn filter(ptr: *mut c_char, gc: *mut GarbageCollection) {
        todo!()
    }
}

impl<L: 'static + RawLock, T: 'static> POp for Lock<L, T> {
    type Object<'o> = &'o Mutex<L, T>;
    type Input = ();
    type Output<'o> = Frozen<MutexGuard<'o, L, T>>;
    type Error = !;

    fn run<'o>(
        &'o mut self,
        mtx: Self::Object<'o>,
        input: Self::Input,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let token = self.lock.run(&mtx.lock, (), pool).unwrap();
        Ok(Frozen::from(MutexGuard {
            mtx,
            unlock: &mut self.unlock,
            token,
            pool,
            _marker: Default::default(),
        }))
    }

    fn reset(&mut self, nested: bool) {
        // `MutexGuard`가 살아있을 때 이 함수 호출은 컴파일 타임에 막아짐.
        self.lock.reset(nested);
    }
}

unsafe impl<L: RawLock, T> Send for Lock<L, T> {}

/// TODO: doc
#[derive(Debug)]
pub struct MutexGuard<'l, L: RawLock, T> {
    mtx: &'l Mutex<L, T>,
    unlock: &'l mut L::Unlock,
    token: L::Token,
    pool: &'static PoolHandle,
    _marker: PhantomData<*const ()>, // !Send + !Sync
}

impl<L: RawLock, T> Drop for MutexGuard<'_, L, T> {
    fn drop(&mut self) {
        let _ = self
            .unlock
            .run(&self.mtx.lock, self.token.clone(), self.pool);
    }
}

impl<L: RawLock, T> Deref for MutexGuard<'_, L, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mtx.data.get() }
    }
}

impl<L: RawLock, T> DerefMut for MutexGuard<'_, L, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mtx.data.get() }
    }
}

impl<'l, L: RawLock, T> MutexGuard<'l, L, T> {
    /// 보호된 데이터의 접근 권한을 얻고 unlock을 예약함.
    ///
    /// # Safety
    ///
    /// `Mutex::data`에 대한 `POp`들은 `Lock`보다 나중에 reset 되어야 함.
    /// 이유: 그렇지 않으면, 서로 다른 스레드가 `MutexGuard`를 각각 가지고 있을 때 모두 fresh `POp`을 수행할 수 있으므로 mutex가 깨짐.
    pub unsafe fn defer_unlock(guard: Frozen<MutexGuard<'l, L, T>>) -> Self {
        guard.own()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crossbeam_utils::thread;

    use crate::plocation::{ralloc::GarbageCollection, PoolHandle};

    use super::*;
    use std::{collections::VecDeque, marker::PhantomData, os::raw::c_char};

    type Queue<T> = VecDeque<T>;

    struct PushPop<L: RawLock, T> {
        lock: Lock<L, Queue<T>>,
        resetting: bool,
        _marker: PhantomData<T>, // TODO: T를 위한 임시. 원래는 POp인 Push<T>, Pop<T>가 있어야 함.
    }

    impl<L: RawLock, T> Default for PushPop<L, T> {
        fn default() -> Self {
            Self {
                lock: Default::default(),
                resetting: false,
                _marker: PhantomData,
            }
        }
    }

    impl<L: RawLock, T> Collectable for PushPop<L, T> {
        unsafe extern "C" fn filter(ptr: *mut c_char, gc: *mut GarbageCollection) {
            todo!()
        }
    }

    impl<L, T> POp for PushPop<L, T>
    where
        L: 'static + RawLock,
        T: 'static + Clone,
    {
        type Object<'o> = &'o Mutex<L, Queue<T>>;
        type Input = T;
        type Output<'o> = Option<T>;
        type Error = !;

        // TODO: 쓰임새를 보이는 용도로 VecDequeue의 push_back(), pop_front()를 사용.
        //       이들은 PersistentOp이 아니므로 이 run()은 지금은 idempotent 하지 않음.
        fn run<'o>(
            &'o mut self,
            queue: Self::Object<'o>,
            input: Self::Input,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            if self.resetting {
                self.reset(false);
            }

            // Lock the object
            let guard = self.lock.run(queue, (), pool).unwrap();
            let mut q = unsafe { MutexGuard::defer_unlock(guard) };

            // Push & Pop
            q.push_back(input);
            Ok(q.pop_front())
        } // Unlock when `q` is dropped

        fn reset(&mut self, nested: bool) {
            if !nested {
                self.resetting = true;
            }

            self.lock.reset(true);

            // sequential queue로써 임시로 사용하고 있는 VecDeque은 persistent version이 아니므로
            // push/pop에 해당하는 POp이 없음
            todo!("reset Push and Pop");

            #[allow(unreachable_code)]
            if !nested {
                self.resetting = false;
            }
        }
    }

    /// Mutex queue에 push/pop 연산하는 테스트
    /// 한 스레드가 lock을 잡고 value를 queue에 넣었다 빼므로
    /// 반드시 같은 값이 나와야 함
    pub(crate) fn test_push_pop_queue<L: 'static + RawLock>(nr_thread: usize, cnt: usize) {
        let q = Mutex::<L, Queue<usize>>::from(Queue::<_>::default()); // TODO(persistent location)
        let mut push_pops: Vec<Vec<PushPop<L, usize>>> = (0..nr_thread)
            .map(|_| (0..cnt).map(|_| PushPop::default()).collect())
            .collect(); // TODO(persistent location)

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..nr_thread {
                let q = &q;
                let push_pops = unsafe {
                    (push_pops.get_unchecked_mut(tid) as *mut Vec<PushPop<L, usize>>)
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
