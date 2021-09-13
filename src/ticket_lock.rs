//! Persistent ticket lock

use std::{
    collections::BinaryHeap,
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};
use epoch::{Owned, Pointer, Shared};
use etrace::some_or;

use crate::{
    list::{self, List},
    lock::{LockOp, RawLock},
    persistent::*,
};

/// TicketLock은 1부터 시작. 0은 ticket이 없음을 표현하기 위해 예약됨.
/// 이는 초기에 ticket을 발급받지 않은 것과 이전에 받은 ticket을 구별하기 위함
const NO_TICKET: usize = 0;
const TICKET_LOCK_INIT: usize = 1;
const TICKET_JUMP: usize = 1;

#[derive(Debug)]
struct Membership {
    ticket: usize, // TODO: atomic?
    trying: bool,  // TODO: atomic?
}

impl Default for Membership {
    fn default() -> Self {
        Self {
            ticket: NO_TICKET,
            trying: false,
        }
    }
}

impl Membership {
    fn ticket(&self) -> Option<usize> {
        if self.ticket == NO_TICKET {
            None
        } else {
            Some(self.ticket)
        }
    }

    #[inline]
    fn is_ticketing(&self) -> bool {
        self.ticket == NO_TICKET && self.trying
    }
}

/// TicketLock의 lock()을 수행하는 Persistent Op.
// TODO: Drop 될 때 membership을 해제해야 함
#[derive(Debug, Default)]
pub struct LockUnlock {
    membership: Atomic<Membership>,
    register: list::Insert<usize, usize>,
    registered: bool,
}

impl<'l> POp<&'l TicketLock> for LockUnlock {
    type Input = LockOp<usize>;
    type Output = Option<usize>;

    fn run(&mut self, lock: &'l TicketLock, op: Self::Input) -> Self::Output {
        match op {
            LockOp::Lock => Some(lock.lock(self)),
            LockOp::Unlock(t) => {
                lock.unlock(t);
                None
            }
        }
    }

    fn reset(&mut self, _nested: bool) {
        unimplemented!()
    }

    // TODO: membership 재활용을 위해선 `reset_weak`이 필요할 것임
}

impl LockUnlock {
    #[inline]
    fn id(&self) -> usize {
        self as *const Self as usize
    }
}

/// TicketLock의 unlock()을 수행하는 Persistent Op.
#[derive(Debug, Default)]
pub struct Unlock;

impl<'l> POp<&'l TicketLock> for Unlock {
    type Input = usize;
    type Output = ();

    fn run(&mut self, lock: &'l TicketLock, ticket: Self::Input) -> Self::Output {
        lock.unlock(ticket)
    }

    fn reset(&mut self, _nested: bool) {}
}

/// IMPORTANT: ticket의 overflow는 없다고 가정
#[derive(Debug)]
pub struct TicketLock {
    curr: AtomicUsize,
    next: AtomicUsize,
    members: List<usize, usize>,
}

impl Default for TicketLock {
    fn default() -> Self {
        Self {
            curr: AtomicUsize::new(TICKET_LOCK_INIT),
            next: AtomicUsize::new(TICKET_LOCK_INIT),
            members: Default::default(),
        }
    }
}

impl TicketLock {
    fn lock(&self, client: &mut LockUnlock) -> usize {
        let guard = epoch::pin();

        let mut m = client.membership.load(Ordering::SeqCst, &guard);
        if m.is_null() {
            // membership 생성
            let n = Owned::new(Membership::default()).into_shared(&guard);
            client.membership.store(n, Ordering::SeqCst);
            m = n;
        }

        // membership 등록: "(key: id, value: membership 포인터)"를 멤버리스트에 삽입
        let inserted = client
            .register
            .run(&self.members, (client.id(), m.into_usize())); // insert는 한 번만 일어남 (thanks to POp)
        debug_assert!(inserted);

        let membership = unsafe { m.deref_mut() };
        let t = membership.ticket();
        let ticket = if let Some(v) = t {
            if membership.trying {
                membership.trying = false;
            }
            v
        } else {
            if membership.trying {
                // post-crash
                membership.trying = false;
                self.recover()
            }

            membership.trying = true;
            membership.ticket = self.next.fetch_add(TICKET_JUMP, Ordering::SeqCst); // where a crash matters
            membership.trying = false;
            membership.ticket
        };

        while ticket < self.curr.load(Ordering::SeqCst) {
            // Back-off
        }

        ticket
    }

    fn recover(&self) {
        // 현재 next와 curr를 캡처
        let end = self.next.load(Ordering::SeqCst);
        let mut start = self.curr.load(Ordering::SeqCst);

        // 멤버들 중에서 start와 end 사이에 있는 티켓 가진 애들 전부 취합 (문제: 멤버가 끝도 없이 늘어날 수도 있음)
        let snapshot = self
            .members
            .head()
            .fold(BinaryHeap::<usize>::default(), |mut acc, mptr| {
                let m: Shared<'_, Membership> = unsafe { Shared::from_usize(mptr) };
                let membership = unsafe { m.deref() };

                // 현재 티켓 뽑고 있는 애는 기다려야 함
                while membership.is_ticketing() {}

                let t = membership.ticket().unwrap();
                if start <= t && t < end {
                    acc.push(t);
                }
                acc
            })
            .into_sorted_vec();
        let mut it = snapshot.iter().skip_while(|t| {
            let now = start;
            start += TICKET_JUMP;
            now != **t
        });

        loop {
            // 잃어버린 티켓 찾음 -> 없으면 복구 끝
            let lost = *some_or!(it.next(), return);

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

    fn unlock(&self, ticket: usize) {
        let curr = self.curr.load(Ordering::SeqCst);
        assert!(ticket <= curr); // for idempotency of `Unlock::run()`
        if curr == ticket {
            self.curr.store(ticket.wrapping_add(1), Ordering::SeqCst);
        }
    }
}

impl RawLock for TicketLock {
    type Token = usize; // ticket
    type LockUnlock<'l> = LockUnlock;
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
        test_push_pop_queue::<TicketLock>(NR_THREAD, COUNT);
    }
}
