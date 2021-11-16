//! Persistent ticket lock

use std::{
    collections::BinaryHeap,
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_utils::Backoff;
use etrace::some_or;

use crate::{
    list::{self, List},
    lock::RawLock,
    pepoch::{self as epoch, atomic::Pointer, Guard, PAtomic, POwned, PShared},
    persistent::*,
    plocation::{
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// TicketLock은 1부터 시작. 0은 ticket이 없음을 표현하기 위해 예약됨.
/// 이는 초기에 ticket을 발급받지 않은 것과 이전에 받은 ticket을 구별하기 위함
const NO_TICKET: usize = 0;
const TICKET_LOCK_INIT: usize = 1;
const TICKET_JUMP: usize = 1;

#[derive(Debug, PartialEq)]
enum State {
    Ready,
    Trying,
    Recovering,
}

#[derive(Debug)]
struct Membership {
    ticket: usize, // TODO: atomic?
    state: State,
}

impl Default for Membership {
    fn default() -> Self {
        Self {
            ticket: NO_TICKET,
            state: State::Ready,
        }
    }
}

impl Membership {
    #[inline]
    fn is_ticketing(&self) -> bool {
        self.ticket == NO_TICKET && self.state == State::Trying
    }
}

/// TicketLock의 lock()을 수행하는 Persistent Op.
// TODO: Drop 될 때 membership을 해제해야 함
#[derive(Debug, Default)]
pub struct Lock {
    membership: PAtomic<Membership>,
    register: list::InsertFront<usize, usize>,
    registered: bool,
}

impl Collectable for Lock {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl POp for Lock {
    type Object<'o> = &'o TicketLock;
    type Input = ();
    type Output<'o> = usize; // ticket
    type Error = !;

    fn run<'o>(
        &'o mut self,
        lock: Self::Object<'o>,
        _: Self::Input,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin();
        Ok(lock.lock(self, &guard, pool))
    }

    // TODO: reset을 해도 membership까지 reset 되거나 할당 해제되진 않을 것임 (state->Ready, ticket->NO_TICKET)
    //       이것이 디자인의 일관성을 깨진 않는지?
    fn reset(&mut self, _: bool, pool: &PoolHandle) {
        let guard = epoch::pin();

        let mut m = self.membership.load(Ordering::SeqCst, &guard);
        if m.is_null() {
            return;
        }

        let m_ref = unsafe { m.deref_mut(pool) };
        m_ref.state = State::Ready;
    }
}

impl Lock {
    #[inline]
    fn id(&self) -> usize {
        self as *const Self as usize
    }
}

/// TicketLock의 unlock()을 수행하는 Persistent Op.
#[derive(Debug, Default)]
pub struct Unlock;

impl Collectable for Unlock {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl POp for Unlock {
    type Object<'l> = &'l TicketLock;
    type Input = usize;
    type Output<'l> = ();
    type Error = !;

    fn run<'o>(
        &'o mut self,
        lock: Self::Object<'o>,
        ticket: Self::Input,
        _: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        lock.unlock(ticket);
        Ok(())
    }

    fn reset(&mut self, _: bool, _: &PoolHandle) {}
}

/// IMPORTANT: ticket의 overflow는 없다고 가정
#[derive(Debug)]
pub struct TicketLock {
    curr: AtomicUsize,
    next: AtomicUsize,
    members: List<usize, usize>, // TODO: 안 쓰이는 membership 청소해야 함 (방법 구상)
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
    fn lock(&self, client: &mut Lock, guard: &Guard, pool: &'static PoolHandle) -> usize {
        let mut m = client.membership.load(Ordering::SeqCst, guard);

        if !client.registered {
            if m.is_null() {
                // membership 생성
                let n = POwned::new(Membership::default(), pool).into_shared(guard);
                client.membership.store(n, Ordering::SeqCst);
                m = n;
            }

            // membership 등록: "(key: id, value: membership 포인터)"를 멤버리스트에 삽입
            if client
                .register
                .run(&self.members, (client.id(), m.into_usize()), pool)
                .is_err()
            {
                unreachable!("Unique client ID as a key")
            }

            client.registered = true; // TODO: list insert에서 "recovery할 때만 해야할지 생각하기" 해결 후 지워도 되는지 확인
        }

        let m_ref = unsafe { m.deref_mut(pool) };
        loop {
            match m_ref.state {
                State::Ready => {
                    if m_ref.ticket != NO_TICKET {
                        m_ref.ticket = NO_TICKET;
                    }
                    m_ref.state = State::Trying;
                    m_ref.ticket = self.next.fetch_add(TICKET_JUMP, Ordering::SeqCst); // where a crash matters
                    break;
                }
                State::Trying => {
                    if m_ref.ticket != NO_TICKET {
                        break;
                    }

                    m_ref.state = State::Recovering;
                }
                State::Recovering => {
                    self.recover(guard, pool);
                    m_ref.state = State::Ready;
                }
            };
        }

        let backoff = Backoff::default();
        // let a = self.curr.load(Ordering::SeqCst);
        while self.curr.load(Ordering::SeqCst) < m_ref.ticket {
            // println!("{} < {}", membership.ticket, a);
            backoff.snooze();
        }

        m_ref.ticket
    }

    fn recover(&self, guard: &Guard, pool: &PoolHandle) {
        // 현재 next와 curr를 캡처
        let end = self.next.load(Ordering::SeqCst);
        let mut start = self.curr.load(Ordering::SeqCst);

        let mut snapshot = BinaryHeap::<usize>::default();
        'snap: loop {
            // 멤버들 중에서 start와 end 사이에 있는 티켓 가진 애들 전부 취합 ( TODO 문제: 멤버가 끝도 없이 늘어날 수도 있음)
            let mut cursor = self.members.head(guard);

            let mut n = cursor.lookup(pool);
            while let Some((_, m_raw)) = n {
                let m: PShared<'_, Membership> = unsafe { PShared::from_usize(*m_raw) };
                let m_ref = unsafe { m.deref(pool) };

                // 현재 티켓 뽑고 있는 애는 기다려야 함
                while m_ref.is_ticketing() {}

                let t = m_ref.ticket;
                if start <= t && t < end {
                    snapshot.push(t);
                }

                if cursor.next(guard, pool).is_err() {
                    snapshot.clear();
                    continue 'snap;
                }

                n = cursor.next(guard, pool).unwrap();
            }
            break;
        }

        let snapshot = snapshot.into_sorted_vec();
        let mut it = snapshot.iter().skip_while(|t| {
            let now = start;
            start += TICKET_JUMP;
            now == **t
        });

        loop {
            // 잃어버린 티켓 찾음 -> 없으면 복구 끝
            let lost = *some_or!(it.next(), return);

            if lost < self.curr.load(Ordering::SeqCst) {
                // 잃어버린 티켓이 아니었던 거임.
                // 멤버십 순회 전에 범위에 있던 애가 일 마치고 티켓을 다시 뽑은 경우
                continue;
            }

            // curr가 티켓에 도달할 때까지 기다림
            while lost > self.curr.load(Ordering::SeqCst) {
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
    type Lock = Lock;
    type Unlock = Unlock;
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::{lock::tests::ConcurAdd, utils::tests::run_test};

    use super::*;

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn concur_add() {
        const FILE_NAME: &str = "ticket_concur_add.pool";
        run_test::<ConcurAdd<TicketLock, NR_THREAD, COUNT>, _>(FILE_NAME, FILE_SIZE)
    }
}
