//! Persistent ticket lock

use std::{
    collections::BinaryHeap,
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};
use etrace::some_or;

use crate::{list::List, lock::RawLock, persistent::*};

/// TicketLock은 1부터 시작. 0은 ticket이 없음을 표현하기 위해 예약됨.
/// 이는 초기에 ticket을 발급받지 않은 것과 이전에 받은 ticket을 구별하기 위함
const NO_TICKET: usize = 0;
const TICKET_LOCK_INIT: usize = 1;
const TICKET_JUMP: usize = 1;

struct State {

}

#[derive(Debug)]
struct Membership {
    id: usize,
    ticket: usize,   // TODO: atomic?
    ticketing: bool, // TODO: atomic?
}

impl Membership {
    #[allow(dead_code)]
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
// TODO: Drop 될 때 membership을 해제해야 함
#[derive(Debug, Default)]
pub struct Lock {
    // TODO: 구현
    membership: Atomic<Membership>,
    registered: bool,
}

impl<'l> POp<&'l TicketLock> for Lock {
    type Input = ();
    type Output = (usize, usize);

    fn run(&mut self, lock: &'l TicketLock, _: Self::Input) -> Self::Output {
        lock.lock(self)
    }

    fn reset(&mut self, _nested: bool) {
        unimplemented!()
    }
}

impl Lock {
    #[inline]
    fn id(&self) -> usize {
        self as *const Self as usize
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct Unlock {
    // TODO: 구현
}

impl<'l> POp<&'l TicketLock> for Unlock {
    type Input = (usize, usize);
    type Output = ();

    fn run(&mut self, lock: &'l TicketLock, _: Self::Input) -> Self::Output {
        lock.unlock()
    }

    fn reset(&mut self, _nested: bool) {
        unimplemented!()
    }
}

/// IMPORTANT: ticket의 overflow는 없다고 가정
#[derive(Debug)]
pub struct TicketLock {
    curr: AtomicUsize,
    next: AtomicUsize,
    members: List<usize, Atomic<Membership>>, // TODO: 비효율?
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
    fn lock(&self, client: &mut Lock) -> (usize, usize) {
        let guard = epoch::pin();
        let id = client.id();
        let mut m = client.membership.load(Ordering::SeqCst, &guard);

        if m.is_null() {
            // TODO: membership 만들기
        }

        if !client.registered {
            // TODO: membership 등록하기
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

        while ticket < self.curr.load(Ordering::SeqCst) {
            // Back-off
        }

        (id, ticket)
    }

    fn recover(&self) {
        // 현재 next와 curr를 캡처
        let end = self.next.load(Ordering::SeqCst);
        let start = self.curr.load(Ordering::SeqCst);

        // 멤버들 중에서 start와 end 사이에 있는 티켓 가진 애들 전부 취합 (문제: 멤버가 끝도 없이 늘어날 수도 있음)
        let snapshot = self
            .members
            .head()
            .fold(BinaryHeap::<usize>::default(), |acc, m| {
                // TODO: NO_TICKET && TICKETING 인 애는 상태가 바뀔 때까지 기다려줘야 함
                if start <= m.ticket && m.ticket < end {
                    acc.push(m.ticket);
                }
                acc
            })
            .into_sorted_vec()
            .iter()
            .skip_while(|t| {
                let now = start;
                start += TICKET_JUMP;
                now != **t
            });

        loop {
            // 잃어버린 티켓 찾음 -> 없으면 복구 끝
            let lost = *some_or!(snapshot.next(), return);

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

    fn unlock(&self) {
        unimplemented!()
    }
}

impl RawLock for TicketLock {
    type Token = (usize, usize); // (membership id, ticket)
    type Lock<'l> = Lock;
    type Unlock<'l> = Unlock;
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
