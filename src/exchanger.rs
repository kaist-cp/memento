//! Concurrent exchanger

// TODO(SMR 적용):
// - SMR 만든 후 crossbeam 걷어내기
// - 현재는 persistent guard가 없어서 lifetime도 이상하게 박혀 있음

// TODO(로직 변경: https://cp-git.kaist.ac.kr/persistent-mem/compositional-persistent-object/-/issues/3#note_6979)

// TODO(pmem 사용(#31, #32)):
// - persist를 위해 flush/fence 추가
// - persistent location 위에서 동작

// TODO(Ordering):
// - Ordering 최적화

use chrono::{Duration, Utc};
use core::ptr;
use core::sync::atomic::{AtomicBool, Ordering};
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use std::mem::MaybeUninit;

use crate::persistent::*;

#[derive(Debug)]
struct Node<T> {
    /// 내가 줄 item
    mine: T,

    /// 상대에게서 받아온 item
    yours: MaybeUninit<T>,

    /// exchange 완료 여부 flag
    response: AtomicBool,

    /// exchange 할 상대의 포인터 (단방향)
    partner: Atomic<Node<T>>,
}

impl<T> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            mine: value,
            yours: MaybeUninit::uninit(),
            response: AtomicBool::new(false),
            partner: Atomic::null(),
        }
    }
}

/// Exchanger의 try exchange 실패
#[derive(Debug, Clone)]
pub struct TryFail;

/// `Exchanger::exchange()`의 시간 제한
#[derive(Debug)]
pub enum Timeout {
    /// `Duration` 만큼 시간 제한
    Limited(Duration),

    /// 시간 제한 없음
    Unlimited,
}

trait ExchangeType<T> {
    fn node(&self) -> &Atomic<Node<T>>;
}

/// Exchanger의 try exchange operation
///
/// `timeout` 시간 내에 교환이 이루어지지 않으면 실패.
/// Try exchange의 결과가 `TryFail`일 경우, 재시도 시 exchanger의 상황과 관계없이 언제나 `TryFail`이 됨.
#[derive(Debug)]
pub struct TryExchange<T> {
    /// exchange item을 담고 다른 스레드 공유하기 위해 할당된 node
    node: Atomic<Node<T>>,
}

impl<T> Default for TryExchange<T> {
    fn default() -> Self {
        Self {
            node: Atomic::null(),
        }
    }
}

impl<T> ExchangeType<T> for TryExchange<T> {
    #[inline]
    fn node(&self) -> &Atomic<Node<T>> {
        &self.node
    }
}

unsafe impl<T> Send for TryExchange<T> {}

impl<T: Clone> PersistentOp for TryExchange<T> {
    type Object = Exchanger<T>;
    type Input = (T, Duration);
    type Output = Result<T, TryFail>;

    fn run(&mut self, xchg: &Self::Object, (value, timeout): Self::Input) -> Self::Output {
        xchg.exchange(self, value, Timeout::Limited(timeout))
    }

    fn reset(&mut self, _: bool) {
        self.node.store(Shared::null(), Ordering::SeqCst);
        // TODO: if not finished -> free node
        // TODO: if node has not been freed, free node
    }
}

impl<T> TryExchange<T> {
    /// 기본값 태그
    const DEFAULT: usize = 0;

    /// Try exchange 결과 실패를 표시하기 위한 태그
    const FAIL: usize = 1;

    /// - 만약 input이 있다면 input은 남겨두고 try 실행을 안 한 것처럼 리셋함.
    /// - 만약 exchange가 이미 성공했다면 내가 교환할 값은 남기고 상대에게 받은 값을 삭제.
    pub fn reset_weak(&self) {
        let guard = epoch::pin();
        let node = self.node.load(Ordering::SeqCst, &guard);

        if node.tag() == Self::FAIL {
            self.node
                .store(node.with_tag(Self::DEFAULT), Ordering::SeqCst);
            return;
        }

        todo!("try exchange 중인 스레드가 exchanger 안에 있다가 crash 났을 경우 reset_weak과 helping의 race가 있을 수 있음");

        // let node_ref = unsafe { node.deref() };
        // if node_ref.response.load(Ordering::SeqCst) {
        //     node_ref.response.store(false, Ordering::SeqCst);
        // }
    }
}

/// Exchanger의 exchange operation.
/// 반드시 exchange에 성공함.
#[derive(Debug)]
pub struct Exchange<T> {
    /// exchange item을 담고 다른 스레드 공유하기 위해 할당된 node
    node: Atomic<Node<T>>,
}

impl<T> Default for Exchange<T> {
    fn default() -> Self {
        Self {
            node: Atomic::null(),
        }
    }
}

impl<T> ExchangeType<T> for Exchange<T> {
    #[inline]
    fn node(&self) -> &Atomic<Node<T>> {
        &self.node
    }
}

unsafe impl<T> Send for Exchange<T> {}

impl<T: Clone> PersistentOp for Exchange<T> {
    type Object = Exchanger<T>;
    type Input = T;
    type Output = T;

    fn run(&mut self, xchg: &Self::Object, value: Self::Input) -> Self::Output {
        xchg.exchange(self, value, Timeout::Unlimited).unwrap()
    }

    fn reset(&mut self, _: bool) {
        self.node.store(Shared::null(), Ordering::SeqCst);
        // TODO: if not finished -> free node
        // TODO: if node has not been freed, free node
    }
}

/// 스레드 간의 exchanger
/// 내부에 마련된 slot을 통해 스레드들끼리 값을 교환함
#[derive(Debug)]
pub struct Exchanger<T> {
    slot: Atomic<Node<T>>,
}

impl<T> Default for Exchanger<T> {
    fn default() -> Self {
        Self {
            // 기존 논문에선 시작 slot이 Default Node임
            // 장황한 구현 및 공간 낭비의 이유로 null로 바꿈
            slot: Atomic::null(),
        }
    }
}

impl<T> Exchanger<T> {
    fn exchange<C: ExchangeType<T>>(
        &self,
        client: &mut C,
        value: T,
        timeout: Timeout,
    ) -> Result<T, TryFail> {
        let guard = epoch::pin();

        let mut myop = client.node().load(Ordering::SeqCst, &guard);

        if myop.is_null() {
            // myop이 null이면 node 할당이 안 된 것이다
            let n = Owned::new(Node::from(value)).into_shared(&guard);
            client.node().store(n, Ordering::SeqCst);
            myop = n;
        } else if myop.tag() == TryExchange::<T>::FAIL {
            // tag가 FAIL이면 try exchange 실패했던 것이다
            return Err(TryFail);
        }

        let myop_ref = unsafe { myop.deref() };

        let start_time = Utc::now();
        loop {
            const WAITING: usize = 0; // default
            const BUSY: usize = 1;

            // slot의 상태에 따른 case는 총 네 가지
            // - Case 1 (null)    : slot에 아무도 없음
            // - Case 2 (WAITING) : slot에서 내 node가 기다림
            // - Case 3 (WAITING) : slot에서 다른 node가 기다림
            // - Case 4 (BUSY)    : slot에서 누군가가 짝짓기 중 (나일 수도 있음)
            let yourop = self.slot.load(Ordering::SeqCst, &guard);

            // 내 교환이 이미 끝났다면, 상대에게 가져온 값을 반환함
            if myop_ref.response.load(Ordering::SeqCst) {
                return Ok(unsafe { Self::finish(myop_ref) });
            }

            // timeout check
            // NOTE: 이 로직은 내가 기다리는 상황이 아닐 때에도 cas를 시도할 수도 있으므로 비효율적임.
            //       로직 변경할 때 어차피 바뀔 로직이므로 이대로 방치.
            if let Timeout::Limited(t) = timeout {
                let now = Utc::now();
                if now.signed_duration_since(start_time) > t {
                    // slot 비우기
                    if self
                        .slot
                        .compare_exchange(
                            myop,
                            Shared::null(),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        )
                        .is_ok()
                    {
                        return Err(TryFail);
                    }

                    // 누군가를 helping 함
                    let yourop = self.slot.load(Ordering::SeqCst, &guard);
                    if yourop.tag() == BUSY {
                        self.help(yourop, &guard);
                    }

                    // helping 대상이 나일 수도 있으므로 마지막 확인
                    if myop_ref.response.load(Ordering::SeqCst) {
                        return Ok(unsafe { Self::finish(myop_ref) });
                    }

                    return Err(TryFail);
                }
            }

            if yourop.is_null() {
                // Case 1: slot에 아무도 없음

                // 내 node를 slot에 넣기 시도
                let _ = self.slot.compare_exchange(
                    yourop,
                    myop,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    &guard,
                );
                continue;
            }

            match yourop.tag() {
                WAITING if myop == yourop => {
                    // Case 2: slot에서 내 node가 기다림
                }
                WAITING => {
                    // Case 3: slot에서 다른 node가 기다림

                    // slot에 있는 node를 짝꿍 삼기 시도
                    myop_ref.partner.store(yourop, Ordering::SeqCst);
                    if self
                        .slot
                        .compare_exchange(
                            yourop,
                            myop.with_tag(BUSY), // "짝짓기 중"으로 표시
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        )
                        .is_ok()
                    {
                        self.help(myop, &guard);
                        return Ok(unsafe { Self::finish(myop_ref) });
                    }
                }
                BUSY => {
                    // Case 4: slot에서 누군가가 짝짓기 중 (나일 수도 있음)
                    self.help(yourop, &guard);
                }
                _ => {
                    unreachable!("Tag is either WAITING or BUSY");
                }
            }
        }
    }

    /// 짝짓기 된 pair를 교환시켜 줌
    fn help(&self, yourop: Shared<'_, Node<T>>, guard: &Guard) {
        let yourop_ref = unsafe { yourop.deref() };
        let partner = yourop_ref.partner.load(Ordering::SeqCst, guard);
        let partner_ref = unsafe { partner.deref() };

        // 두 node가 교환할 값을 서로에게 복사
        // write-write race가 일어날 수 있음. 그러나 같은 값을 write하게 되므로 상관 없음.
        unsafe {
            let lval = ptr::read(&yourop_ref.mine as *const _);
            let rval = ptr::read(&partner_ref.mine as *const _);
            (yourop_ref.yours.as_ptr() as *mut T).write(rval);
            (partner_ref.yours.as_ptr() as *mut T).write(lval);
        }

        yourop_ref.response.store(true, Ordering::SeqCst);
        partner_ref.response.store(true, Ordering::SeqCst);

        // slot 비우기
        let _ = self.slot.compare_exchange(
            yourop,
            Shared::null(),
            Ordering::SeqCst,
            Ordering::SeqCst,
            guard,
        );
    }

    /// 상대에게서 받아온 item을 반환
    // TODO: 이게 unsafe일 이유는 없을 것 같음
    unsafe fn finish(myop_ref: &Node<T>) -> T {
        ptr::read(myop_ref.yours.as_ptr())
        // TODO: free node
    }
}

unsafe impl<T> Send for Exchanger<T> {}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicUsize;

    use chrono::Duration;
    use crossbeam_utils::thread;
    use serial_test::serial;

    use super::*;

    #[test]
    fn exchange_once() {
        let xchg: Exchanger<usize> = Exchanger::default(); // TODO(persistent location)
        let mut exchanges: Vec<Exchange<usize>> = (0..2).map(|_| Default::default()).collect(); // TODO(persistent location)

        // 아래 로직은 idempotent 함
        #[allow(box_pointers)]
        thread::scope(|scope| {
            let xchg = &xchg;
            for tid in 0..2 {
                let exchange = unsafe {
                    (exchanges.get_unchecked_mut(tid) as *mut Exchange<usize>)
                        .as_mut()
                        .unwrap()
                };
                let _ = scope.spawn(move |_| {
                    // `move` for `tid`
                    let ret = exchange.run(xchg, tid);
                    assert_eq!(ret, 1 - tid);
                });
            }
        })
        .unwrap();
    }

    /// Before rotation : [0]  [1]  [2]
    /// After rotation  : [1]  [2]  [0]
    #[test]
    fn rotate_left() {
        let (mut item0, mut item1, mut item2) = (0, 1, 2); // TODO(persistent location)

        let lxhg = Exchanger::<i32>::default(); // TODO(persistent location)
        let rxhg = Exchanger::<i32>::default(); // TODO(persistent location)

        let mut exchange0 = Exchange::<i32>::default(); // TODO(persistent location)
        let mut exchange2 = Exchange::<i32>::default(); // TODO(persistent location)

        let mut exchange1_0 = Exchange::<i32>::default(); // TODO(persistent location)
        let mut exchange1_2 = Exchange::<i32>::default(); // TODO(persistent location)

        // 아래 로직은 idempotent 함
        #[allow(box_pointers)]
        thread::scope(|scope| {
            let _ = scope.spawn(|_| {
                // [0] -> [1]    [2]
                item0 = exchange0.run(&lxhg, item0);
                assert_eq!(item0, 1);
            });

            let _ = scope.spawn(|_| {
                // [0]    [1] <- [2]
                item2 = exchange2.run(&rxhg, item2);
                assert_eq!(item2, 0);
            });

            // Composition in the middle
            // Step1: [0] <- [1]    [2]
            item1 = exchange1_0.run(&lxhg, item1);
            assert_eq!(item1, 0);

            // Step2: [1]    [0] -> [2]
            item1 = exchange1_2.run(&rxhg, item1);
            assert_eq!(item1, 2);
        })
        .unwrap();
    }

    /// 여럿이서 exchange 하다가 혼자만 남은 tid와 exchange한 횟수
    #[derive(Default)]
    struct Unfinished {
        flag: AtomicUsize,
        tid: AtomicUsize,
        cnt: AtomicUsize,
    }

    /// 스레드 여러 개의 exchange
    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn exchange_many() {
        const NR_THREAD: usize = 4;
        const COUNT: usize = 1_000_000;

        let xchg: Exchanger<usize> = Exchanger::default(); // TODO(persistent location)
        let mut exchanges: Vec<Vec<TryExchange<usize>>> = (0..NR_THREAD)
            .map(|_| (0..COUNT).map(|_| Default::default()).collect())
            .collect(); // TODO(persistent location)

        // 아래 로직은 idempotent 함

        let unfinished = Unfinished::default();

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..NR_THREAD {
                let xchg = &xchg;
                let unfinished = &unfinished;
                let exchanges = unsafe {
                    (exchanges.get_unchecked_mut(tid) as *mut Vec<TryExchange<usize>>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    // `move` for `tid`
                    for (i, exchange) in exchanges.iter_mut().enumerate() {
                        if let Err(_) = exchange.run(xchg, (tid, Duration::milliseconds(500))) {
                            // 긴 시간 동안 exchange 안 되면 혼자 남은 것으로 판단
                            // => 스레드 혼자 남을 경우 더 이상 global exchange 진행 불가
                            if unfinished.flag.fetch_add(1, Ordering::SeqCst) == 0 {
                                unfinished.tid.store(tid, Ordering::SeqCst);
                                unfinished.cnt.store(i, Ordering::SeqCst);
                            }
                            break;
                        }
                    }
                });
            }
        })
        .unwrap();

        // Validate test
        let u_flag = unfinished.flag.load(Ordering::SeqCst);
        let u_tid_cnt = if u_flag == 1 {
            Some((
                unfinished.tid.load(Ordering::SeqCst),
                unfinished.cnt.load(Ordering::SeqCst),
            ))
        } else if u_flag == 0 {
            None
        } else {
            // 끝까지 하지 못한 스레드가 둘 이상일 경우 무효
            return;
        };

        // Gather results
        let mut results = vec![0_usize; NR_THREAD];
        let expected: Vec<usize> = (0..NR_THREAD)
            .map(|tid| match u_tid_cnt {
                Some((u_tid, u_cnt)) if tid == u_tid => u_cnt,
                Some(_) | None => COUNT,
            })
            .collect();

        for (tid, exchanges) in exchanges.iter_mut().enumerate() {
            for (i, exchange) in exchanges.iter_mut().enumerate() {
                if i == expected[tid] {
                    break;
                }
                let ret = exchange
                    .run(&xchg, (666, Duration::milliseconds(0)))
                    .unwrap(); // 이미 끝난 op이므로 (1) dummy input은 영향 없고 (2) 반드시 리턴.
                results[ret] += 1;
            }
        }

        // Check results
        assert!(results
            .iter()
            .enumerate()
            .all(|(tid, r)| *r == expected[tid]));
    }
}
