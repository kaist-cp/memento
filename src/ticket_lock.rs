//! Persistent ticket lock

use std::{
    cell::UnsafeCell,
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};
use etrace::some_or;

use crate::{list::List, persistent::*};

/// TicketLock의 ticket은 짝수만 갖게 됨
/// 이는 초기에 ticket을 발급받지 않은 것과 이전에 받은 ticket을 구별하기 위함
// TODO: ticket이 한 바퀴 돈 건 어떻게 하지?
const TICKET_LOCK_INIT: usize = 0;
const TICKET_JUMP: usize = 2;
const NO_TICKET: usize = 1;

#[derive(Debug)]
struct Membership {
    id: usize,
    ticket: usize,   // TODO: atomic?
    ticketing: bool, // TODO: atomic?
}

impl Membership {
    fn new(id: usize) -> Self {
        Self {
            id,
            ticket: NO_TICKET,
            ticketing: false,
        }
    }

    fn ticket(&self) -> Option<usize> {
        if self.ticket == NO_TICKET {
            None
        } else {
            Some(self.ticket)
        }
    }
}

/// TicketLock의 lock()을 수행하는 Persistent Op.
/// Guard를 얼려서 반환하므로 unlock을 하기 위해선 Guard::defer_unlock()을 호출해야 함.
///
/// # Examples
///
/// ```rust
/// // Assume these are on persistent location:
/// let x = TicketLock<usize>::default();
/// let lock = Lock;
///
/// {
///     let guard = lock.run(&x, ());
///     let _guard = Guard::defer_unlock(guard);
///
///     ... // Critical section
/// } // Unlock when `_guard` is dropped
/// ```
// TODO: Drop 될 때 membership을 해제해야 함
#[derive(Debug, Default)]
pub struct Lock {
    // TODO: 구현
    membership: Atomic<Membership>,
    registered: bool,
}

impl<'l, T> POp<&'l TicketLock<T>> for Lock {
    type Input = ();
    type Output = Frozen<Guard<'l, T>>;

    fn run(&mut self, lock: &'l TicketLock<T>, _: Self::Input) -> Self::Output {
        Frozen::from(lock.lock(self))
    }

    #[allow(unused_variables)]
    fn reset(&mut self, nested: bool) {
        unimplemented!()
    }
}

/// TODO: doc
#[derive(Debug)]
pub struct TicketLock<T> {
    inner: UnsafeCell<T>,
    curr: AtomicUsize,
    next: AtomicUsize,
    members: List<usize, Atomic<Membership>>, // TODO: 비효율?
}

impl<T> From<T> for TicketLock<T> {
    fn from(value: T) -> Self {
        Self {
            inner: UnsafeCell::from(value),
            curr: AtomicUsize::new(TICKET_LOCK_INIT),
            next: AtomicUsize::new(TICKET_LOCK_INIT),
            members: Default::default(),
        }
    }
}

impl<T> TicketLock<T> {
    fn lock<'l>(&'l self, client: &'l mut Lock) -> Guard<'l, T> {
        let guard = epoch::pin();
        // let id = client.id(); // TODO: id 쓸 일 있나?
        let m = client.membership.load(Ordering::SeqCst, &guard);

        if m.is_null() {
            // TODO: membership 만들기
        }

        if !client.registered {
            // TODO: membership 등록하기
            // self.register(client.id());
        }

        let membership = unsafe { m.deref_mut() };
        let t = membership.ticket();
        let ticket = if let Some(v) = t {
            if membership.ticketing {
                membership.ticketing = false;
            }
            v
        } else {
            if membership.ticketing {
                // post-crash
                self.recover()
            } else {
                membership.ticketing = true;
            }

            membership.ticket = self.next.fetch_add(TICKET_JUMP, Ordering::SeqCst); // where a crash matters
            membership.ticketing = false;
            membership.ticket
        };

        // TODO: 이미 써버린 티켓일 때

        while ticket != self.curr.load(Ordering::SeqCst) {
            // Back-off
        }

        Guard {
            lock: &self,
            op: client,
        }
    }

    fn register(&self, id: usize) -> Option<&Membership> {
        unimplemented!()
    }

    fn recover(&self) {
        // 현재 next를 캡처
        let bound = self.next.load(Ordering::SeqCst);

        // 멤버들 중에서 next보다 작은 애들 전부 취합 (문제1: overflow, 문제2: 멤버가 끝도 없이 늘어날 수도 있음)
        // TODO

        loop {
            // 잃어버린 티켓 찾음 -> 없으면 복구 끝
            let lost = some_or!(self.find_lost(bound), return);

            // curr가 티켓에 도달할 때까지 기다림
            while lost != self.curr.load(Ordering::SeqCst) {
                // Back-off
            }

            // CAS로 잃어버린 티켓을 건너뛰게 해줌
            // 성공하면 잃어버린 티켓이 자기꺼였다고 간주하고 리턴
            // (뒤에 잃어버린 티켓이 더 있을 수 있지만 그건 다른 복구 스레드의 소관임)
            if self
                .curr
                .compare_exchange(
                    lost,
                    lost.wrapping_add(TICKET_JUMP),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    fn find_lost(&self, bound: usize) -> Option<usize> {
        unimplemented!()
    }

    fn unlock(&self, client: &mut Lock) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::lock::tests::*;

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn push_pop_queue() {
        test_push_pop_queue::<TicketLock<_>>(NR_THREAD, COUNT);
    }
}
