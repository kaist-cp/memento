//! Concurrent exchanger

// TODO(pmem 사용(#31, #32)):
// - persist를 위해 flush/fence 추가
// - persistent location 위에서 동작

// TODO(Ordering):
// - Ordering 최적화

// TODO(SMR 적용):
// - SMR 만든 후 crossbeam 걷어내기
// - 현재는 persistent guard가 없어서 lifetime도 이상하게 박혀 있음

// TODO(로직 변경):
// - 기존 로직 단점
//   + slot이 빔: 굳이 slot을 비우는 상태를 거침
//   + 짝짓기 의존성: 누군가의 짝짓기(복사 과정)가 끝난 뒤에야 global progress
//   + post-crash 스레드를 염두한 불필요한 atomic load가 많음
// - 변경 로직 아이디어
//   + slot에 있는 노드에게 partner가
//     * 있다면 상호 참조 시켜주고, slot을 자기꺼로 cas (짝짓기 생략 + slot이 비워지지 않음)
//     * 없다면 자기를 partner로 가리키도록 cas
//   + 호출할 때마다 partner node에서 value 복사함 (partner가 준 값 저장소 불필요 + atomic response 불필요)

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

/// `Exchanger`의 `exchange()`을 호출할 때 쓰일 client
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
        self.node = Atomic::null();
    }
}

unsafe impl<T: Send> Send for ExchangeClient<T> {}

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
    /// 나의 값을 주고 상대의 값을 반환함
    /// node에 주요 정보를 넣고 다른 스레드에게 보여주어 helping 할 수 있음
    // TODO: client.mine != val인 경우에 대한 정책
    //       => 논의 결과: 상관 없음 (safe하기만 하면 됨. functional correctness는 보장 안 함)
    //       => 추후 persistent_op trait으로 주석 이동
    pub fn exchange(&self, client: &mut ExchangeClient<T>, val: T) -> T {
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

        let myop_ref = unsafe { myop.deref() };

        loop {
            // slot은 네 가지 상태 중 하나임
            let yourop = self.slot.load(Ordering::SeqCst, guard);

            // 그 전에 내 교환은 이미 끝났 건지 확인
            if myop_ref.response.load(Ordering::SeqCst) {
                return unsafe { Self::finish(myop_ref) };
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

            match yourop.tag() {
                0 if myop != yourop => {
                    // (2) slot에서 다른 node가 기다림

                    // slot에 있는 node를 짝꿍 삼기 시도
                    myop_ref.partner.store(yourop, Ordering::SeqCst);
                    if self
                        .slot
                        .compare_exchange(
                            yourop,
                            myop.with_tag(1), // "짝짓기 중"으로 표시
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        self.help(myop, guard);
                        return unsafe { Self::finish(myop_ref) };
                    }
                }
                0 => {
                    // (3) slot에서 내 node가 기다림
                }
                1 => {
                    // (4) 짝짓기 중
                    self.help(yourop, guard);
                }
                _ => {
                    unreachable!("Tag is at most 1");
                }
            }
        }
    }

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

    unsafe fn finish(myop_ref: &Node<T>) -> T {
        ptr::read(myop_ref.yours.as_ptr())
        // TODO: free node
    }
}

#[cfg(test)]
mod test {
    use std::{process::exit, sync::atomic::AtomicUsize, time::Duration};

    use super::*;
    use crossbeam_utils::thread;

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
                    let ret = xchg_ref.exchange(client, tid);
                    assert_eq!(ret, 1 - tid);
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

    // 스레드 여러 개의 exchange
    #[test]
    fn exchange_many() {
        const NR_THREAD: usize = 4;
        const COUNT: usize = 1_000_000;

        let xchg: Exchanger<usize> = Exchanger::default(); // TODO(persistent location)
        let mut clients = vec![vec!(ExchangeClient::<usize>::default(); COUNT); NR_THREAD]; // TODO(persistent location)

        // 아래 로직은 idempotent 함

        let mut cnts = vec![];
        let mut results = vec![];
        for _ in 0..NR_THREAD {
            cnts.push(AtomicUsize::new(0));
            results.push(AtomicUsize::new(0));
        }

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..NR_THREAD {
                let xchg_ref = &xchg;
                let cnts_ref = &cnts;
                let results_ref = &results;
                let client_vec = unsafe {
                    (clients.get_unchecked_mut(tid) as *mut Vec<ExchangeClient<usize>>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    // `move` for `tid`
                    for i in 0..COUNT {
                        let ret = xchg_ref.exchange(&mut client_vec[i], tid);

                        let _ = results_ref[ret].fetch_add(1, Ordering::SeqCst);
                        let _ = cnts_ref[tid].fetch_add(1, Ordering::SeqCst);
                    }
                });
            }

            // Wait for all works to be done
            // TODO: exchanger에 time limit 기능 추가한 뒤 혼자 남은 스레드는 알아서 포기하게끔
            let mut remained_tid = 0;
            let mut remained_cnt = 0;
            'wait: loop {
                std::thread::sleep(Duration::from_millis(500));

                for (tid, cnt) in cnts.iter().enumerate() {
                    let c = cnt.load(Ordering::SeqCst);
                    if c != COUNT {
                        if remained_cnt == 0 {
                            remained_cnt = COUNT - c;
                            remained_tid = tid;
                        } else {
                            remained_cnt = 0;
                            continue 'wait;
                        }
                    }
                }

                let _ = results[remained_tid].fetch_add(remained_cnt, Ordering::SeqCst);
                break;
            }

            // Check results
            for result in results.iter() {
                let r = result.load(Ordering::SeqCst);
                assert_eq!(r, COUNT);
            }

            // TODO: exchanger에 time limit 기능 추가한 뒤 제거
            exit(0);
        })
        .unwrap();
    }
}
