// TODO(pmem 사용(#31, #32)):
// - persist를 위해 flush/fence 추가
// - persistent location 위에서 동작

// TODO(SMR 적용):
// - SMR 만든 후 crossbeam 걷어내기
// - 현재는 persistent guard가 없어서 lifetime도 이상하게 박혀 있음

// TODO(로직 변경):
// - 기존: switch_pair를 한 뒤에야 slot을 비움
// - 변경: switch_pair 하지 않아도 slot 비울 수 있음
// - 아이디어:
//   + partner들끼리 상호 참조만 잘 된다면 switch_pair를 하지 않아도 slot을 비울 수 있음
//   + switch_pair가 됐는지 안 됐는지 제대로 확인하는 방법이 필요 (response가 한 쪽만 true될 경우를 주의)

use core::ptr;
use core::sync::atomic::{AtomicBool, Ordering};
use crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared};
use std::mem::MaybeUninit;

use crate::persistent::*;

#[derive(Debug, PartialEq, Eq)]
enum State {
    Waiting,
    Busy,
}

#[derive(Debug)]
struct Node<'p, T> {
    /// slot에서의 상태
    state: State,

    /// 내가 줄 item
    mine: T,

    /// 상대에게서 받아온 item
    yours: MaybeUninit<T>,

    /// exchange 완료 여부 flag
    response: AtomicBool,

    /// exchange 할 상대의 포인터 (단방향)
    // Shared인 이유: SMR을 위함. 상대가 자기 client를 free해도 참조할 수 있어야 함.
    partner: Shared<'p, Node<'p, T>>,

    /// partner 참조를 위한 persistent guard
    guard: Guard,
}

impl<T> Node<'_, T> {
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

/// `Exchanger`의 `exchange()`을 호출할 때 쓰일 client
#[derive(Debug)]
pub struct ExchangeClient<'p, T> {
    /// 해당 op를 위해 할당된 node
    node: Shared<'p, Node<'p, T>>,

    /// node 참조를 위한 persistent guard
    guard: Guard,
}

impl<T> Default for ExchangeClient<'_, T> {
    fn default() -> Self {
        Self {
            node: Shared::null(),
            guard: pin(),
        }
    }
}

impl<T> PersistentClient for ExchangeClient<'_, T> {
    fn reset(&mut self) {
        self.node = Shared::null();
        self.guard = pin();
    }
}

unsafe impl<T: Send> Send for ExchangeClient<'_, T> {}

/// 스레드 간의 exchanger
/// 내부에 마련된 slot을 통해 스레드들끼리 값을 교환함
#[derive(Debug)]
pub struct Exchanger<'p, T> {
    slot: Atomic<Node<'p, T>>,
}

impl<T> Default for Exchanger<'_, T> {
    fn default() -> Self {
        Self {
            // 기존 논문에선 시작 slot이 Default Node임
            // 장황한 구현 및 공간 낭비의 이유로 null로 바꿈
            slot: Atomic::null(),
        }
    }
}

unsafe impl<T: Send> Sync for Exchanger<'_, T> {}
unsafe impl<T: Send> Send for Exchanger<'_, T> {}

impl<'p, T> Exchanger<'p, T> {
    /// 나의 값을 주고 상대의 값을 반환함
    /// node에 주요 정보를 넣고 다른 스레드에게 보여주어 helping 할 수 있음
    // TODO: client.mine != val인 경우에 대한 정책
    //       => 논의 결과: 상관 없음 (safe하기만 하면 됨. functional correctness는 보장 안 함)
    //       => 추후 persistent_op trait으로 주석 이동
    pub fn exchange(&self, client: &'p mut ExchangeClient<'p, T>, val: T) -> T {
        if client.node.is_null() {
            // Install a helping struct for the first execution
            let n = Owned::new(Node {
                state: State::Waiting,
                mine: val,
                yours: MaybeUninit::uninit(),
                response: AtomicBool::new(false),
                partner: Shared::null(),
                guard: pin(),
            });

            client.node = n.into_shared(&client.guard);
        }

        self._exchange(client.node)
        // TODO: free node
    }

    /// 나의 값을 주고 상대의 값을 반환하는 코어 로직
    fn _exchange(&self, myop: Shared<'p, Node<'p, T>>) -> T {
        let myop_ref = unsafe { myop.deref() };
        let guard = &myop_ref.guard;

        loop {
            let yourop = self.slot.load(Ordering::SeqCst, guard);
            if yourop.is_null() {
                // slot에 아무도 없음
                if myop_ref.response.load(Ordering::SeqCst) {
                    // 내 목표는 달성함 -> 졸업
                    return unsafe { ptr::read(myop_ref.yours.as_ptr()) };
                }

                // 내가 slot에 들어가서 누군가를 기다려야 함
                if myop_ref.state == State::Busy {
                    unsafe { ptr::write(&myop_ref.state as *const _ as *mut _, State::Waiting) };
                }
                let _ = self.slot.compare_exchange(
                    yourop,
                    myop,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
                continue;
            }

            let yourop_ref = unsafe { yourop.deref() };
            match yourop_ref.state {
                State::Waiting if myop != yourop => {
                    // slot에서 누가 기다리고 있음
                    if myop_ref.response.load(Ordering::SeqCst) {
                        // 내 목표는 달성함 -> 졸업
                        return unsafe { ptr::read(myop_ref.yours.as_ptr()) };
                    }

                    // 짝짓기 시도
                    unsafe {
                        ptr::drop_in_place(
                            &myop_ref.partner as *const _ as *mut Shared<'_, Node<'_, T>>,
                        );
                        ptr::write(&myop_ref.partner as *const _ as *mut _, yourop);
                        ptr::write(&myop_ref.state as *const _ as *mut _, State::Busy);
                    }
                    let _ = self.slot.compare_exchange(
                        yourop,
                        myop,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                }
                State::Busy => {
                    // 이미 짝짓기가 시작됨 -> helping
                    let yourop_part_ref = unsafe { yourop_ref.partner.as_ref().unwrap() };
                    Node::switch_pair(yourop_ref, yourop_part_ref);

                    let _ = self.slot.compare_exchange(
                        yourop,
                        Shared::null(),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );

                    if myop_ref.response.load(Ordering::SeqCst) {
                        // 내 목표는 달성함 -> 졸업
                        return unsafe { ptr::read(myop_ref.yours.as_ptr()) };
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crossbeam_utils::thread;

    #[test]
    fn exchange_once() {
        let xchg: Exchanger<'_, usize> = Exchanger::default();

        // array를 못 쓰는 이유: guard가 clone이 안 됨
        let mut exclient0 = ExchangeClient::<usize>::default(); // persistent
        let mut exclient1 = ExchangeClient::<usize>::default(); // persistent

        // 아래 로직은 idempotent 함
        #[allow(box_pointers)]
        thread::scope(|scope| {
            let xchg_ref = &xchg;
            let exclient_ref0 = &mut exclient0;
            let exclient_ref1 = &mut exclient1;

            let _ = scope.spawn(move |_| {
                let ret = xchg_ref.exchange(exclient_ref0, 0);
                assert_eq!(ret, 1);
            });

            let ret = xchg_ref.exchange(exclient_ref1, 1);
            assert_eq!(ret, 0);
        })
        .unwrap();
    }

    // Before rotation : [0]  [1]  [2]
    // After rotation  : [1]  [2]  [0]
    #[test]
    fn rotate_left() {
        let (mut item0, mut item1, mut item2) = (0, 1, 2); // persistent

        let lxhg = Exchanger::<i32>::default(); // persistent
        let rxhg = Exchanger::<i32>::default(); // persistent

        let mut exclient0 = ExchangeClient::<i32>::default(); // persistent
        let mut exclient2 = ExchangeClient::<i32>::default(); // persistent

        let mut exclient1_0 = ExchangeClient::<i32>::default(); // persistent
        let mut exclient1_2 = ExchangeClient::<i32>::default(); // persistent

        // 아래 로직은 idempotent 함
        #[allow(box_pointers)]
        thread::scope(|scope| {
            let _ = scope.spawn(|_| {
                // [0] -> [1]    [2]
                item0 = lxhg.exchange(&mut exclient0, item0);
                assert_eq!(item0, 1);
            });

            let _ = scope.spawn(|_| {
                // [0]    [1] <- [2]
                item2 = rxhg.exchange(&mut exclient2, item2);
                assert_eq!(item2, 0);
            });

            // Composition in the middle
            // Step1: [0] <- [1]    [2]
            item1 = lxhg.exchange(&mut exclient1_0, item1);
            assert_eq!(item1, 0);

            // Step2: [1]    [0] -> [2]
            item1 = rxhg.exchange(&mut exclient1_2, item1);
            assert_eq!(item1, 2);
        })
        .unwrap();
    }

    // 스레드 여러 개의 exchange (array를 쓰지 못해 우선 코멘팅)
    // #[test]
    // fn exchange_many() {
    //     let xchg: Exchanger<'_, usize> = Exchanger::new();
    //     let tcnt = AtomicUsize::new(0);

    //     #[allow(box_pointers)]
    //     thread::scope(|scope| {
    //         for tid in 0..NR_THREAD {
    //             let xchg_ref = &xchg;
    //             let tcnt_ref = &tcnt;

    //             let _ = scope.spawn(move |_| {
    //                 for _ in 1..=COUNT {
    //                     let mut exinfo = ExchangeInfo::<usize>::new();
    //                     let _ = xchg_ref.exchange(&mut exinfo, tid);
    //                 }
    //                 let _ = tcnt_ref.fetch_add(1, Ordering::SeqCst);
    //             });
    //         }
    //         loop {
    //             std::thread::sleep(Duration::from_millis(500));
    //             let finish_cnt = tcnt.load(Ordering::SeqCst);
    //             if finish_cnt == NR_THREAD - 1 || finish_cnt == NR_THREAD {
    //                 // case NR_THREAD - 1: Unable to progress
    //                 // case NR_THREAD: All done
    //                 exit(0);
    //             }
    //         }
    //     })
    //     .unwrap();
    // }
}
