//! Persistent ticket lock

use std::{cell::UnsafeCell, fmt::Debug, ops::{Deref, DerefMut}, sync::atomic::{AtomicUsize, Ordering}};

use crossbeam_epoch::{self as epoch, Atomic};

use crate::{list::List, persistent::*};

/// TODO: doc
#[derive(Debug)]
pub struct Guard<'l, T> {
    lock: &'l TicketLock<T>
    // TODO: token
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        // TODO: 구현
    }
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.inner.get() }
    }
}

impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.inner.get() }
    }
}

impl<'l, T> Guard<'l, T> {
    /// TODO: doc
    pub fn defer_unlock(guard: Frozen<Guard<'l, T>>) -> Self {
        unsafe { guard.own() }
    }
}

/// TicketLock의 ticket은 짝수만 갖게 됨
/// 이는 초기에 ticket을 발급받지 않은 것과 이전에 받은 ticket을 구별하기 위함
// TODO: ticket이 한 바퀴 돈 건 어떻게 하지?
const TICKET_LOCK_INIT: usize = 0;
const TICKET_JUMP: usize = 2;
const NO_TICKET: usize = 1;

#[derive(Debug)]
struct Membership {
    id: usize,
    ticket: usize, // TODO: atomic?
    ticketing: bool, // TODO: atomic?
}

impl Membership {
    fn new(id: usize) -> Self {
        Self {
            id,
            ticket: NO_TICKET,
            ticketing: false
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
    registered: bool
}

impl<'l, T> POp<&'l TicketLock<T>> for Lock {
    type Input = ();
    type Output = Frozen<Guard<'l, T>>;

    // TODO: 구현
    fn run(&mut self, lock: &'l TicketLock<T>, _: Self::Input) -> Self::Output {
        Frozen::from(Guard { lock })
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
    members: List<usize, Atomic<Membership>> // TODO: 비효율?
}

impl<T> From<T> for TicketLock<T> {
    fn from(value: T) -> Self {
        Self {
            inner: UnsafeCell::from(value),
            curr: AtomicUsize::new(TICKET_LOCK_INIT),
            next: AtomicUsize::new(TICKET_LOCK_INIT),
            members: Default::default()
        }
    }
}

impl<T> TicketLock<T> {
    fn lock(&self, client: Lock) -> Guard<'_, T> {
        let guard = epoch::pin();
        let id = client.id(); // TODO: id 쓸 일 있나?
        let m = client.membership.load(Ordering::SeqCst, &guard);

        if m.is_null() {
            // TODO: membership 만들기
        }

        if !client.registered {
            // TODO: membership 등록하기
            self.register(client.id());
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
        }

        while ticket != self.curr.load(Ordering::SeqCst) {
            // Back-off
        }

        Guard {
            lock: &self
        }
    }

    fn register(&self, id: usize) -> Option<&Membership> {
        unimplemented!()
    }

    fn recover(&self) {
        unimplemented!()
        // 1. 현재 next를 캡처
        // 2. 멤버들 중에서 next보다 작은 애들 전부 취합 (문제1: overflow, 문제2: 멤버가 끝도 없이 늘어날 수도 있음)
        // 3. 구멍 찾음
        // 4. curr가 구멍에 도달할 때까지 기다렸다가 구멍에 도달하면 CAS로 구멍을 스킵시켜줌
        // 5. CAS 성공하거나 curr가 1.에서 캡처한 next보다 커지면 리턴
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_utils::thread;

    use super::*;
    use std::{collections::VecDeque, marker::PhantomData};

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    type Queue<T> = VecDeque<T>;
    type LockBasedQueue<T> = TicketLock<Queue<T>>;

    struct PushPop<T> {
        lock: Lock,
        resetting: bool,
        _marker: PhantomData<T> // TODO: T를 위한 임시. 원래는 POp인 Push<T>, Pop<T>가 있어야 함.
    }

    impl<T> Default for PushPop<T> {
        fn default() -> Self {
            Self {
                lock: Default::default(),
                resetting: false,
                _marker: PhantomData
            }
        }
    }

    impl<T: Clone> POp<&LockBasedQueue<T>> for PushPop<T> {
        type Input = T;
        type Output = Option<T>;

        // TODO: 쓰임새를 보이는 용도로 VecDequeue의 push_back(), pop_back()를 사용.
        //       이들은 PersistentOp이 아니므로 이 run()은 지금은 idempotent 하지 않음.
        fn run(&mut self, queue: &LockBasedQueue<T>, input: Self::Input) -> Self::Output {
            if self.resetting {
                self.reset(false);
            }

            // Lock the object
            let guard = self.lock.run(queue, ());
            let q = Guard::defer_unlock(guard);

            // Push & Pop
            q.push_back(input);
            q.pop_front()
        } // Unlock when `q` is dropped

        fn reset(&mut self, nested: bool) {
            if !nested {
                self.resetting = true;
            }

            POp::<&LockBasedQueue<T>>::reset(&mut self.lock, true);
            todo!("reset Push and Pop");

            #[allow(unreachable_code)]
            if !nested {
                self.resetting = false;
            }
        }
    }

    #[test]
    fn push_pop_seq_queue() {
        let obj = LockBasedQueue::from(Queue::<usize>::default()); // TODO(persistent location)
        let mut push_pops: Vec<Vec<PushPop<usize>>> = (0..NR_THREAD)
            .map(|_| (0..COUNT).map(|_| PushPop::default()).collect())
            .collect(); // TODO(persistent location)

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..NR_THREAD {
                let obj = &obj;
                let push_pops = unsafe {
                    (push_pops.get_unchecked_mut(tid) as *mut Vec<PushPop<usize>>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    for i in 0..COUNT {
                        // Check if push_pop acts like an identity function
                        // lock 구현 안 되어 있으므로 assertion 실패함
                        assert_eq!(push_pops[i].run(obj, tid), Some(tid));
                    }
                });
            }
        })
        .unwrap();
    }
}
