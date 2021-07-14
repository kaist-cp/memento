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
use crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared};
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

impl<T> Node<T> {
    /// 두 node가 교환할 값을 서로에게 복사
    fn switch_pair(left: &Self, right: &Self) {
        unsafe {
            let lval = ptr::read(&left.mine as *const _);
            let rval = ptr::read(&right.mine as *const _);
            (left.yours.as_ptr() as *mut T).write(rval);
            (right.yours.as_ptr() as *mut T).write(lval);
        }

        left.response.store(true, Ordering::SeqCst);
        right.response.store(true, Ordering::SeqCst);
    }
}

/// `Exchanger::exchange()`을 호출할 때 쓰일 client
/// `Exchanger::exchange(val, timeout)`:
/// - 나의 `val`을 주고 상대의 값을 반환함
/// - `timeout`이 `Timeout::Limited(d)`일 경우 `d` 시간 내에 교환이 이루어지지 않으면 실패
#[derive(Debug, Clone)]
pub struct ExchangeClient<T> {
    /// 해당 op를 위해 할당된 node
    node: Atomic<Node<T>>,
}

impl<T> Default for ExchangeClient<T> {
    fn default() -> Self {
        Self {
            node: Atomic::null(),
        }
    }
}

impl<T> PersistentClient for ExchangeClient<T> {
    fn reset(&mut self) {
        self.node.store(Shared::null(), Ordering::SeqCst);
    }
}

unsafe impl<T: Send> Send for ExchangeClient<T> {}

/// `Exchanger::exchange()`의 시간 제한
#[derive(Debug)]
pub enum Timeout {
    /// `Duration` 만큼 시간 제한
    Limited(Duration),

    /// 시간 제한 없음
    Unlimited,
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

unsafe impl<T: Send> Sync for Exchanger<T> {}
unsafe impl<T: Send> Send for Exchanger<T> {}

impl<T> Exchanger<T> {
    // TODO: 계속 copy가 일어나는 것에 대해 ownership 관련 이슈 만들기 (문제상황정리, 답도 찾기)
    fn exchange(&self, client: &mut ExchangeClient<T>, val: T, timeout: Timeout) -> Result<T, ()> {
        let guard = &pin();
        let mut myop = client.node.load(Ordering::SeqCst, guard);

        if myop.is_null() {
            // First execution
            let n = Owned::new(Node {
                mine: val,
                yours: MaybeUninit::uninit(),
                response: AtomicBool::new(false),
                partner: Atomic::null(),
            });

            myop = n.into_shared(guard);
            client.node.store(myop, Ordering::SeqCst);
        }

        let start_time = Utc::now();
        let myop_ref = unsafe { myop.deref() };

        loop {
            // slot은 네 가지 상태 중 하나임
            let yourop = self.slot.load(Ordering::SeqCst, guard);

            // 내 교환은 이미 끝났 건지 확인
            if myop_ref.response.load(Ordering::SeqCst) {
                return Ok(unsafe { Self::finish(myop_ref) });
            }

            // timeout check
            if let Timeout::Limited(t) = timeout {
                let now = Utc::now();
                if now.signed_duration_since(start_time) > t {
                    if myop != yourop {
                        return Err(());
                    }

                    // slot 비우기 -> 실패한다면 그새 짝이 생겼다는 뜻
                    if self
                        .slot
                        .compare_exchange(
                            myop,
                            Shared::null(),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        return Err(());
                    }
                }
            }

            if yourop.is_null() {
                // (1) slot에 아무도 없음

                // 내 node를 slot에 넣기 시도
                let _ = self.slot.compare_exchange(
                    yourop,
                    myop,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
                continue;
            }

            const WAITING: usize = 0; // default
            const BUSY: usize = 1;

            match yourop.tag() {
                WAITING if myop == yourop => {
                    // (2) slot에서 내 node가 기다림
                }
                WAITING => {
                    // (3) slot에서 다른 node가 기다림

                    // slot에 있는 node를 짝꿍 삼기 시도
                    myop_ref.partner.store(yourop, Ordering::SeqCst);
                    if self
                        .slot
                        .compare_exchange(
                            yourop,
                            myop.with_tag(BUSY), // "짝짓기 중"으로 표시
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        self.help(myop, guard);
                        return Ok(unsafe { Self::finish(myop_ref) });
                    }
                }
                BUSY => {
                    // (4) 짝짓기 중
                    self.help(yourop, guard);
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
        Node::switch_pair(yourop_ref, partner_ref);

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
    unsafe fn finish(myop_ref: &Node<T>) -> T {
        ptr::read(myop_ref.yours.as_ptr())
        // TODO: free node
    }
}

impl<T> PersistentOp<ExchangeClient<T>> for Exchanger<T> {
    type Input = (T, Timeout);
    type Output = Result<T, ()>;

    fn persistent_op(&self, client: &mut ExchangeClient<T>, input: Self::Input) -> Self::Output {
        self.exchange(client, input.0, input.1)
    }
}

#[cfg(test)]
mod test {
    use chrono::Duration;
    use crossbeam_utils::thread;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[test]
    fn exchange_once() {
        let xchg: Exchanger<usize> = Exchanger::default(); // TODO(persistent location)
        let mut clients = vec![ExchangeClient::<usize>::default(); 2]; // TODO(persistent location)

        // 아래 로직은 idempotent 함
        #[allow(box_pointers)]
        thread::scope(|scope| {
            let xchg_ref = &xchg;
            for tid in 0..2 {
                let client = unsafe {
                    (clients.get_unchecked_mut(tid) as *mut ExchangeClient<usize>)
                        .as_mut()
                        .unwrap()
                };
                let _ = scope.spawn(move |_| {
                    // `move` for `tid`
                    let ret = xchg_ref.persistent_op(client, (tid, Timeout::Unlimited));
                    assert_eq!(ret, Ok(1 - tid));
                });
            }
        })
        .unwrap();
    }

    // Before rotation : [0]  [1]  [2]
    // After rotation  : [1]  [2]  [0]
    #[test]
    fn rotate_left() {
        let (mut item0, mut item1, mut item2) = (0, 1, 2); // TODO(persistent location)

        let lxhg = Exchanger::<i32>::default(); // TODO(persistent location)
        let rxhg = Exchanger::<i32>::default(); // TODO(persistent location)

        let mut exclient0 = ExchangeClient::<i32>::default(); // TODO(persistent location)
        let mut exclient2 = ExchangeClient::<i32>::default(); // TODO(persistent location)

        let mut exclient1_0 = ExchangeClient::<i32>::default(); // TODO(persistent location)
        let mut exclient1_2 = ExchangeClient::<i32>::default(); // TODO(persistent location)

        // 아래 로직은 idempotent 함
        #[allow(box_pointers)]
        thread::scope(|scope| {
            let _ = scope.spawn(|_| {
                // [0] -> [1]    [2]
                item0 = lxhg
                    .persistent_op(&mut exclient0, (item0, Timeout::Unlimited))
                    .unwrap();
                assert_eq!(item0, 1);
            });

            let _ = scope.spawn(|_| {
                // [0]    [1] <- [2]
                item2 = rxhg
                    .persistent_op(&mut exclient2, (item2, Timeout::Unlimited))
                    .unwrap();
                assert_eq!(item2, 0);
            });

            // Composition in the middle
            // Step1: [0] <- [1]    [2]
            item1 = lxhg
                .persistent_op(&mut exclient1_0, (item1, Timeout::Unlimited))
                .unwrap();
            assert_eq!(item1, 0);

            // Step2: [1]    [0] -> [2]
            item1 = rxhg
                .persistent_op(&mut exclient1_2, (item1, Timeout::Unlimited))
                .unwrap();
            assert_eq!(item1, 2);
        })
        .unwrap();
    }

    // 스레드 여러 개의 exchange
    #[test]
    fn exchange_many() {
        const NR_THREAD: usize = 4;
        const COUNT: usize = 1_000_000;

        let xchg: Exchanger<usize> = Exchanger::default(); // TODO(persistent location)
        let mut clients = vec![vec!(ExchangeClient::<usize>::default(); COUNT); NR_THREAD]; // TODO(persistent location)

        // 아래 로직은 idempotent 함

        // tid별 실행횟수 vec 및
        // return 값별 리턴개수 vec initialization
        let mut exec_cnts = vec![];
        let mut ret_cnts = vec![];
        for _ in 0..NR_THREAD {
            exec_cnts.push(AtomicUsize::new(0));
            ret_cnts.push(AtomicUsize::new(0));
        }

        // 혼자만 남은 tid와 남은 횟수
        let remained_tid = AtomicUsize::new(0);
        let remained_cnt = AtomicUsize::new(0);

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..NR_THREAD {
                let xchg_ref = &xchg;
                let client_vec = unsafe {
                    (clients.get_unchecked_mut(tid) as *mut Vec<ExchangeClient<usize>>)
                        .as_mut()
                        .unwrap()
                };

                let exec_cnts_ref = &exec_cnts;
                let ret_cnts_ref = &ret_cnts;

                let remained_tid_ref = &remained_tid;
                let remained_cnt_ref = &remained_cnt;

                let _ = scope.spawn(move |_| {
                    // `move` for `tid`
                    for (i, client) in client_vec.iter_mut().enumerate() {
                        let ret = xchg_ref.persistent_op(
                            client,
                            (tid, Timeout::Limited(Duration::milliseconds(5000))), // 충분히 긴 시간
                        );
                        let ret = ok_or!(ret, {
                            // 스레드 혼자 남을 경우 더 이상 global exchange 진행 불가
                            remained_tid_ref.store(tid, Ordering::SeqCst);
                            remained_cnt_ref.store(COUNT - i, Ordering::SeqCst);
                            break;
                        });
                        let _ = exec_cnts_ref[tid].fetch_add(1, Ordering::SeqCst);
                        let _ = ret_cnts_ref[ret].fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        })
        .unwrap();

        // Check results
        for (tid, r) in ret_cnts.iter().enumerate() {
            let rm_tid = remained_tid.load(Ordering::SeqCst);
            let rm_cnt = remained_cnt.load(Ordering::SeqCst);
            let ret_cnt = r.load(Ordering::SeqCst);

            if tid != rm_tid {
                assert_eq!(ret_cnt, COUNT);
            } else {
                assert_eq!(ret_cnt + rm_cnt, COUNT);
            }
        }
    }
}
