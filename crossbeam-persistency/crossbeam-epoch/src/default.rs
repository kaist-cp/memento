//! The default garbage collector.
//!
//! For each thread, a participant is lazily initialized on its first use, when the current thread
//! is registered in the default collector.  If initialized, the thread's participant will get
//! destructed on thread exit, which in turn unregisters the thread.

use std::cell::RefCell;

use crate::collector::{Collector, LocalHandle};
use crate::guard::Guard;
use crate::primitive::{lazy_static, thread_local};

lazy_static! {
    /// The global data for the default garbage collector.
    static ref COLLECTOR: Collector = Collector::new();
}

thread_local! {
    /// The per-thread participant for the default garbage collector.
    // @anonymous: RefCell은 old_guard에서 원래 쓰던 local을 못찾았을 때 새로 만든 local로 바꿔주기 위함
    static HANDLE: RefCell<LocalHandle> = RefCell::new(COLLECTOR.register(None));
}

/// `tid` 스레드가 사용하던 guard를 반환
///
/// # Safety
///
/// 하나의 `tid`는 한 스레드만 써야함. 예를 들어 스레드 두 개가 둘다 tid 0으로 old guard를 호출하면 안됨.
pub unsafe fn old_guard(tid: usize) -> Guard {
    HANDLE.with(|h| {
        // tid가 사용하던 `Local`을 탐색
        if let Some(handle) = COLLECTOR.find(tid) {
            // repin 도중 crash 났다면 guard가 있었음을 보장
            // 이유: repin 도중 crash 난 경우 pre-crash에서는 guard가 있었는데 post-crash에서는 없다고 착각할 수 있기 때문 (`repin_after()` 로직 참고)
            if handle.is_repinning() {
                // guard가 0개가 아니었음만 보장하면 됨
                // 여기서 guard count를 직접 조정하는 이유는, 아래의 pin 내부 로직에서 guard cnt가 0이냐 아니냐에 따라 로직이 구분되기 때문
                unsafe { handle.set_guard_count(1) }
            }

            // 이전의 맥락에 이어서 guard를 만듦
            // - guard를 다시 만들때 이전의 맥락에 이어서 만드는 게 중요 (i.e. guard 다시 만들 때 이전에 guard가 원래 없었는지 있었는지 구분하여 동작 필요)
            // - 따라서 이전의 guard count에 이어서 pin하고, 그 다음 guard count를 직접 1로 초기화
            let guard = handle.pin();

            // Local의 필드를 적절히 재초기화
            // 이전에 존재하던 Local에서 파생된 obj (LocalHandle, Guard)는 다 사라졌기 때문에, local이 세고 있던 obj 수를 잘 초기화해줘야함
            unsafe { handle.reset_count() }; // Local을 처음 만들 때처럼 초기화
            unsafe { handle.set_guard_count(1) };
            h.replace(handle);
            return guard;
        }

        // tid가 사용하던 `Local`이 없다면 없다면 새로 등록
        h.replace(COLLECTOR.register(Some(tid)));
        return h.borrow().pin();
    })
}

/// Pins the current thread.
#[inline]
pub fn pin() -> Guard {
    with_handle(|handle| handle.borrow().pin())
}

/// Returns `true` if the current thread is pinned.
#[inline]
pub fn is_pinned() -> bool {
    with_handle(|handle| handle.borrow().is_pinned())
}

/// Returns the default global collector.
pub fn default_collector() -> &'static Collector {
    &COLLECTOR
}

#[inline]
fn with_handle<F, R>(mut f: F) -> R
where
    F: FnMut(&RefCell<LocalHandle>) -> R,
{
    HANDLE
        .try_with(|h| f(h))
        .unwrap_or_else(|_| f(&RefCell::new(COLLECTOR.register(None))))
}

#[cfg(all(test, not(crossbeam_loom)))]
mod tests {
    use crate::{pin, unprotected, Owned};
    use crossbeam_utils::thread;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn pin_while_exiting() {
        struct Foo;

        impl Drop for Foo {
            fn drop(&mut self) {
                // Pin after `HANDLE` has been dropped. This must not panic.
                super::pin();
            }
        }

        thread_local! {
            static FOO: Foo = Foo;
        }

        thread::scope(|scope| {
            scope.spawn(|_| {
                // Initialize `FOO` and then `HANDLE`.
                FOO.with(|_| ());
                super::pin();
                // At thread exit, `HANDLE` gets dropped first and `FOO` second.
            });
        })
        .unwrap();
    }

    // old guard의 property 3가지를 모두 만족하는지 테스트
    // 1. 비정상종료(i.e. thread-local panic)시엔 guard를 drop하지 않고 잘 보존하는지
    // 2. 보존된 guard를 old_guard로 잘 가져올 수 있는지
    // 3. 정상종료시엔 guard를 잘 drop하는지
    #[test]
    fn old_guard() {
        use crate::{default_collector, old_guard};
        const THREADS: usize = 4;
        const COUNT: usize = 64;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Elem {}
        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        #[allow(box_pointers)]
        thread::scope(|scope| {
            // Phase 1. 스레드 0은 guard 잡은 후 local crash나서 죽지만 guard는 보존됨
            let handler = scope.spawn(move |_| {
                let guard = unsafe { old_guard(0) };
                // let guard = pin(); // 이 주석 해제하여 old_guard 대신 pin 쓰면, phase 2에서 elem drop이 미뤄지지 못하고 호출되는 것을 볼 수 있음
                panic!();
            });
            let _ = handler.join();

            // Phase 2. 다른 스레드들은 elem drop을 여러 번 예약(defer)하지만, 스레드 0이 잡았던 guard가 남아있으므로 그 어떤 예약된 함수도 실행되지 못함
            let mut handlers = Vec::new();
            for _ in 1..THREADS {
                let h = scope.spawn(move |_| {
                    for _ in 0..COUNT {
                        let guard = &pin();
                        let elem = Owned::new(Elem {}).into_shared(guard);
                        unsafe { guard.defer_destroy(elem) }
                        guard.flush();
                    }
                });
                handlers.push(h);
            }

            while !handlers.is_empty() {
                let _ = handlers.pop().unwrap().join();
                let guard = &pin();
                default_collector().global.collect(guard);
                assert_eq!(DROPS.load(Ordering::Relaxed), 0);
            }

            // Phase 3. 스레드 0이 보존했던 guard를 다시 가져오고 정상종료하면, 이번엔 guard를 보존하지 않고 잘 drop함.
            // 이제 모든 guard가 사라졌으니 이제서야 예약된 함수가 호출 될 수 있음 (i.e. global epoch이 advance 될 수 있음)
            let handler = scope.spawn(move |_| {
                let guard = unsafe { old_guard(0) };
            });
            let _ = handler.join();

            // 직접 advance 및 collect 하여 defer된 drop 호출되게 함
            // 이렇게 직접 하는 이유는, 새로 pin할 때만 advance 및 collect가 호출되기 때문
            default_collector()
                .global
                .try_advance(unsafe { unprotected() });
            default_collector().global.collect(unsafe { unprotected() });
            assert!(DROPS.load(Ordering::Relaxed) != 0);
        })
        .unwrap();
    }
}
