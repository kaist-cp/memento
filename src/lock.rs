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
    type Lock: for<'o> POp<Object<'o> = &'o Self, Input = (), Output<'o> = Self::Token, Error = !>;

    /// Unlock operation을 수행하는 POp
    ///
    /// 실제 token이 아닌 값으로 unlock 호출시 panic
    // TODO: Output에 Frozen을 강제해야 할 수도 있음. MutexGuard 인터페이스 없이 RawLock만으로는 critical section의 mutex 보장 못함.
    type Unlock: for<'o> POp<Object<'o> = &'o Self, Input = Self::Token, Output<'o> = (), Error = !>;
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
        Self {
            lock: Default::default(),
            unlock: Default::default(),
            _marker: Default::default(),
        }
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
        (): Self::Input,
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

    fn reset(&mut self, nested: bool, pool: &'static PoolHandle) {
        // `MutexGuard`가 살아있을 때 이 함수 호출은 컴파일 타임에 막아짐.
        self.lock.reset(nested, pool);
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

    use crate::{
        plocation::{ralloc::GarbageCollection, PoolHandle},
        utils::tests::TestRootOp,
    };

    use super::*;
    use std::os::raw::c_char;

    struct FetchAdd<L: RawLock> {
        lock: Lock<L, usize>,
        fetched: usize,
        state: State, // TODO: 아무래도 `POption<T>`를 만들 필요가...
    }

    enum State {
        Ready,
        Fetched,
        Added,
        Resetting,
    }

    impl<L: RawLock> Default for FetchAdd<L> {
        fn default() -> Self {
            Self {
                lock: Default::default(),
                fetched: 0xDEADBEEF,
                state: State::Ready,
            }
        }
    }

    impl<L: RawLock> Collectable for FetchAdd<L> {
        unsafe extern "C" fn filter(ptr: *mut c_char, gc: *mut GarbageCollection) {
            todo!()
        }
    }

    impl<L> POp for FetchAdd<L>
    where
        L: 'static + RawLock,
    {
        type Object<'o> = &'o Mutex<L, usize>;
        type Input = usize;
        type Output<'o> = usize;
        type Error = !;

        fn run<'o>(
            &'o mut self,
            count: Self::Object<'o>,
            rhs: Self::Input,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            if let State::Resetting = self.state {
                self.reset(false, pool);
            }

            // Lock the object
            let guard = self.lock.run(count, (), pool).unwrap();
            let mut x = unsafe { MutexGuard::defer_unlock(guard) };

            loop {
                match self.state {
                    State::Ready => {
                        self.fetched = *x;
                        self.state = State::Fetched;
                    }
                    State::Fetched => {
                        *x = x.wrapping_add(rhs);
                        self.state = State::Added;
                    }
                    State::Added => {
                        return Ok(self.fetched);
                    }
                    State::Resetting => {
                        unreachable!("reset 중에 lock을 잡을 순 없었을 것임")
                    }
                }
            }
        } // Unlock when `cnt` is dropped

        fn reset(&mut self, nested: bool, pool: &'static PoolHandle) {
            if !nested {
                self.state = State::Resetting;
            }

            self.lock.reset(nested, pool);

            if !nested {
                self.state = State::Ready;
            }
        }
    }

    pub(crate) struct ConcurAdd<L: RawLock, const NR_THREAD: usize, const COUNT: usize> {
        x: Mutex<L, usize>,
        faas: [FetchAdd<L>; NR_THREAD],
    }

    impl<L: RawLock, const NR_THREAD: usize, const COUNT: usize> Default
        for ConcurAdd<L, NR_THREAD, COUNT>
    {
        fn default() -> Self {
            Self {
                x: Mutex::from(0),
                faas: array_init::array_init(|_| FetchAdd::<L>::default()),
            }
        }
    }

    impl<L: RawLock, const NR_THREAD: usize, const COUNT: usize> Collectable
        for ConcurAdd<L, NR_THREAD, COUNT>
    {
        unsafe extern "C" fn filter(ptr: *mut c_char, gc: *mut GarbageCollection) {
            todo!()
        }
    }

    impl<L: 'static + RawLock, const NR_THREAD: usize, const COUNT: usize> POp
        for ConcurAdd<L, NR_THREAD, COUNT>
    {
        type Object<'o> = ();
        type Input = ();
        type Output<'o>
        where
            L: 'o,
        = ();
        type Error = !;

        fn run<'o>(
            &'o mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            // Alias
            let (x, faas) = (&self.x, &mut self.faas);

            #[allow(box_pointers)]
            thread::scope(|scope| {
                for tid in 0..NR_THREAD {
                    let faa = unsafe {
                        (faas.get_unchecked_mut(tid) as *mut FetchAdd<L>)
                            .as_mut()
                            .unwrap()
                    };

                    let _ = scope.spawn(move |_| {
                        for i in 0..COUNT {
                            let _ = faa.run(x, tid + 1, pool);
                            faa.reset(false, pool);
                        }
                    });
                }
            })
            .unwrap();

            let mut tmp_lock = Lock::default();
            let mtx = tmp_lock.run(x, (), pool).unwrap();
            let final_x = unsafe { MutexGuard::defer_unlock(mtx) };
            assert_eq!(*final_x, (NR_THREAD * (NR_THREAD + 1) / 2) * COUNT);
            Ok(())
        }

        fn reset(&mut self, nested: bool, _: &PoolHandle) {
            todo!()
        }
    }

    impl<L: 'static + RawLock, const NR_THREAD: usize, const COUNT: usize> TestRootOp
        for ConcurAdd<L, NR_THREAD, COUNT>
    {
    }
}
