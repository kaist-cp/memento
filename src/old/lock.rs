//! Persistent mutex

use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crossbeam_epoch::{self as epoch, Guard};

use crate::{
    *,
    pmem::{
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// Persistent raw lock
pub trait RawLock: Default + Send + Sync + Collectable {
    /// lock 잡았음을 증명하는 토큰
    type Token: Clone;

    /// Lock operation을 수행하는 Memento
    type Lock: for<'o> Memento<
        Object<'o> = &'o Self,
        Input<'o> = (),
        Output<'o> = Self::Token,
        Error = !,
    >;

    /// Unlock operation을 수행하는 Memento
    ///
    /// 실제 token이 아닌 값으로 unlock 호출시 panic
    // TODO: Output에 Frozen을 강제해야 할 수도 있음. MutexGuard 인터페이스 없이 RawLock만으로는 critical section의 mutex 보장 못함.
    type Unlock: for<'o> Memento<
        Object<'o> = &'o Self,
        Input<'o> = Self::Token,
        Output<'o> = (),
        Error = !,
    >;
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

impl<L: RawLock, T> Collectable for Mutex<L, T> {
    fn filter(mtx: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        L::filter(&mut mtx.lock, gc, pool);
    }
}

impl<L: RawLock> PDefault for Mutex<L, usize> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Self::from(0)
    }
}
/// MutexGuard를 얼려서 반환하므로 사용하기 위해선 Guard::defer_unlock()을 호출해야 함.
///
/// # Examples
///
/// ```rust
/// # use memento::{
/// #   pmem::pool::*,
/// #   *,
/// #   test_utils::tests::get_dummy_handle
/// # };
/// # let pool = get_dummy_handle(8 * 1024 * 1024 * 1024).unwrap();
/// use memento::ticket_lock::TicketLock;
/// use memento::lock::{Mutex, Lock, MutexGuard};
/// use crossbeam_epoch::{self as epoch};
///
/// let x = Mutex::<TicketLock, i32>::from(0);
/// let mut lock = Lock::default();
/// let mut ebr_guard = epoch::pin();
///
/// {
///     let mtx_guard = lock.run(&x, (), &mut ebr_guard, pool).unwrap();
///     let v = unsafe { MutexGuard::defer_unlock(mtx_guard) };
///
///     // ... Critical section
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
    fn filter(lock: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        L::Lock::filter(&mut lock.lock, gc, pool);
        L::Unlock::filter(&mut lock.unlock, gc, pool);
    }
}

impl<L: 'static + RawLock, T: 'static> Memento for Lock<L, T> {
    type Object<'o> = &'o Mutex<L, T>;
    type Input<'o> = ();
    type Output<'o> = Frozen<MutexGuard<'o, L, T>>;
    type Error = !;

    fn run<'o>(
        &'o mut self,
        mtx: Self::Object<'o>,
        (): Self::Input<'o>,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let token = self.lock.run(&mtx.lock, (), guard, pool).unwrap();
        Ok(Frozen::from(MutexGuard {
            mtx,
            unlock: &mut self.unlock,
            token,
            pool,
            _marker: Default::default(),
        }))
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        // `MutexGuard`가 살아있을 때 이 함수 호출은 컴파일 타임에 막아짐.
        self.lock.reset(guard, pool);
    }

    fn set_recovery(&mut self, _: &'static PoolHandle) {}
}

unsafe impl<L: RawLock, T> Send for Lock<L, T>
where
    L::Lock: Send,
    L::Unlock: Send,
{
}

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
        let guard = epoch::pin(); // TODO: run에서 쓰인 guard 안 받고 이래도 되나
        let _ = self
            .unlock
            .run(&self.mtx.lock, self.token.clone(), &guard, self.pool);
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
    /// `Mutex::data`에 대한 `Memento`들은 `Lock`보다 나중에 reset 되어야 함.
    /// 이유: 그렇지 않으면, 서로 다른 스레드가 `MutexGuard`를 각각 가지고 있을 때 모두 fresh `Memento`을 수행할 수 있으므로 mutex가 깨짐.
    pub unsafe fn defer_unlock(guard: Frozen<MutexGuard<'l, L, T>>) -> Self {
        guard.own()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::{
        pmem::{ralloc::GarbageCollection, PoolHandle},
        test_utils::tests::*,
    };

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
        fn filter(faa: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            Lock::filter(&mut faa.lock, gc, pool);
        }
    }

    impl<L> Memento for FetchAdd<L>
    where
        L: 'static + RawLock,
    {
        type Object<'o> = &'o Mutex<L, usize>;
        type Input<'o> = usize;
        type Output<'o> = usize;
        type Error = !;

        fn run<'o>(
            &'o mut self,
            count: Self::Object<'o>,
            rhs: Self::Input<'o>,
            guard: &Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            if let State::Resetting = self.state {
                self.reset(false, guard, pool);
            }

            // Lock the object
            let guard = self.lock.run(count, (), guard, pool).unwrap();
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

        fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
            if !nested {
                self.state = State::Resetting;
            }

            self.lock.reset(guard, pool);

            if !nested {
                self.state = State::Ready;
            }
        }

        fn set_recovery(&mut self, pool: &'static PoolHandle) {
            self.lock.set_recovery(pool);

            // TODO: reset 중이었다가 crash난 애의 reset을 끝내줄 수 있음.
            //       그러면 run에서 reset 중인지 검사 불필요.
        }
    }

    pub(crate) struct ConcurAdd<L: RawLock, const NR_THREAD: usize, const COUNT: usize> {
        faa: FetchAdd<L>,
        cnt: usize,

        // 결과를 확인하기 위한 lock
        check_res: Lock<L, usize>,
    }

    unsafe impl<L: RawLock, const NR_THREAD: usize, const COUNT: usize> Sync
        for ConcurAdd<L, NR_THREAD, COUNT>
    {
    }

    impl<L: RawLock, const NR_THREAD: usize, const COUNT: usize> Default
        for ConcurAdd<L, NR_THREAD, COUNT>
    {
        fn default() -> Self {
            Self {
                faa: FetchAdd::<L>::default(),
                cnt: 0,
                check_res: Lock::default(),
            }
        }
    }

    impl<L: RawLock, const NR_THREAD: usize, const COUNT: usize> Collectable
        for ConcurAdd<L, NR_THREAD, COUNT>
    {
        fn filter(concur_add: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            FetchAdd::filter(&mut concur_add.faa, gc, pool);
            Lock::filter(&mut concur_add.check_res, gc, pool);
        }
    }

    impl<L: 'static + RawLock, const NR_THREAD: usize, const COUNT: usize> Memento
        for ConcurAdd<L, NR_THREAD, COUNT>
    where
        L::Lock: Send,
        L::Unlock: Send,
    {
        type Object<'o> = &'o Mutex<L, usize>;
        type Input<'o> = usize; // tid(mid)
        type Output<'o>
        where
            L: 'o,
        = ();
        type Error = !;

        fn run<'o>(
            &'o mut self,
            x: Self::Object<'o>,
            tid: Self::Input<'o>,
            guard: &Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            match tid {
                0 => {
                    // 다른 스레드들이 다 끝날때까지 기다림
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check result
                    self.check_res.reset(false, guard, pool);
                    let mtx = self.check_res.run(x, (), guard, pool).unwrap();
                    let final_x = unsafe { MutexGuard::defer_unlock(mtx) };
                    assert_eq!(*final_x, (NR_THREAD * (NR_THREAD + 1) / 2) * COUNT);
                }
                _ => {
                    let faa = &mut self.faa;
                    let cnt = &mut self.cnt;

                    assert!(*cnt <= 2 * COUNT);
                    while *cnt < 2 * COUNT {
                        if *cnt & 1 == 0 {
                            let _ = faa.run(x, tid, guard, pool);
                            *cnt += 1;
                        }
                        faa.reset(false, guard, pool);
                        *cnt += 1;
                    }

                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        }

        fn reset(&mut self, _nested: bool, _guard: &Guard, _pool: &'static PoolHandle) {
            todo!()
        }

        fn set_recovery(&mut self, pool: &'static PoolHandle) {
            self.faa.set_recovery(pool);
            // TODO: reset 구현 후 reset 복구도 해줄 수 있나 확인
        }
    }

    impl<L: 'static + RawLock> TestRootObj for Mutex<L, usize> {}
    impl<L: 'static + RawLock, const NR_THREAD: usize, const COUNT: usize>
        TestRootMemento<Mutex<L, usize>> for ConcurAdd<L, NR_THREAD, COUNT>
    where
        L::Lock: Send,
        L::Unlock: Send,
    {
    }
}
