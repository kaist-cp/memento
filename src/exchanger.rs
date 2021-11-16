//! Concurrent exchanger

// TODO(SMR 적용):
// - SMR 만든 후 crossbeam 걷어내기
// - 현재는 persistent guard가 없어서 lifetime도 이상하게 박혀 있음

// TODO(로직 변경: https://cp-git.kaist.ac.kr/persistent-mem/compositional-persistent-object/-/issues/3#note_6979)

// TODO(pmem 사용(#31, #32)):
// - persist를 위해 flush/fence 추가

// TODO(Ordering):
// - Ordering 최적화

use chrono::{Duration, Utc};
use core::ptr;
use core::sync::atomic::{AtomicBool, Ordering};
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, Guard, PAtomic, POwned, PShared};
use crate::persistent::*;
use crate::plocation::pool::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};

// TODO: T가 포인터일 수 있으니 T도 Collectable이여야함
#[derive(Debug)]
struct Node<T> {
    /// 내가 줄 item
    mine: T,

    /// 상대에게서 받아온 item
    yours: MaybeUninit<T>,

    /// exchange 완료 여부 flag
    response: AtomicBool,

    /// exchange 할 상대의 포인터 (단방향)
    partner: PAtomic<Node<T>>,
}

impl<T: Clone> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            mine: value,
            yours: MaybeUninit::uninit(),
            response: AtomicBool::new(false),
            partner: PAtomic::null(),
        }
    }
}

/// Exchanger의 try exchange 실패
#[derive(Debug, Clone)]
pub struct TryFail;

/// `Exchanger::exchange()`의 시간 제한
#[derive(Debug)]
enum Timeout {
    /// `Duration` 만큼 시간 제한
    Limited(Duration),

    /// 시간 제한 없음
    Unlimited,
}

trait ExchangeType<T> {
    fn node(&self) -> &PAtomic<Node<T>>;
}

type ExchangeCond<T> = fn(&T) -> bool;

/// Exchanger의 try exchange operation
///
/// `timeout` 시간 내에 교환이 이루어지지 않으면 실패.
/// Try exchange의 결과가 `TryFail`일 경우, 재시도 시 exchanger의 상황과 관계없이 언제나 `TryFail`이 됨.
#[derive(Debug)]
pub struct TryExchange<T> {
    /// exchange item을 담고 다른 스레드 공유하기 위해 할당된 node
    node: PAtomic<Node<T>>,
}

impl<T> Default for TryExchange<T> {
    fn default() -> Self {
        Self {
            node: PAtomic::null(),
        }
    }
}

impl<T> ExchangeType<T> for TryExchange<T> {
    #[inline]
    fn node(&self) -> &PAtomic<Node<T>> {
        &self.node
    }
}

unsafe impl<T: Send + Sync> Send for TryExchange<T> {}

impl<T: Clone> Collectable for TryExchange<T> {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> Memento for TryExchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input = (T, Duration, ExchangeCond<T>);
    type Output<'o> = T;
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        xchg: Self::Object<'o>,
        (value, timeout, cond): Self::Input,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin();

        xchg.exchange(self, value, Timeout::Limited(timeout), cond, &guard, pool)
    }

    fn reset(&mut self, _: bool, _: &PoolHandle) {
        self.node.store(PShared::null(), Ordering::SeqCst);
        // TODO: if not finished -> free node
        // TODO: if node has not been freed, free node
    }
}

/// Exchanger의 exchange operation.
/// 반드시 exchange에 성공함.
#[derive(Debug)]
pub struct Exchange<T> {
    /// exchange item을 담고 다른 스레드 공유하기 위해 할당된 node
    node: PAtomic<Node<T>>,
}

impl<T> Default for Exchange<T> {
    fn default() -> Self {
        Self {
            node: PAtomic::null(),
        }
    }
}

impl<T> ExchangeType<T> for Exchange<T> {
    #[inline]
    fn node(&self) -> &PAtomic<Node<T>> {
        &self.node
    }
}

unsafe impl<T: Send + Sync> Send for Exchange<T> {}

impl<T: Clone> Collectable for Exchange<T> {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> Memento for Exchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input = (T, ExchangeCond<T>);
    type Output<'o> = T;
    type Error = !;

    fn run<'o>(
        &'o mut self,
        xchg: Self::Object<'o>,
        (value, cond): Self::Input,
        pool: &PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let guard = epoch::pin();

        Ok(xchg
            .exchange(self, value, Timeout::Unlimited, cond, &guard, pool)
            .unwrap()) // 시간 무제한이므로 return 시 반드시 성공을 보장
    }

    fn reset(&mut self, _: bool, _: &PoolHandle) {
        self.node.store(PShared::null(), Ordering::SeqCst);
        // TODO: if not finished -> free node
        // TODO: if node has not been freed, free node
    }
}

/// 스레드 간의 exchanger
/// 내부에 마련된 slot을 통해 스레드들끼리 값을 교환함
#[derive(Debug)]
pub struct Exchanger<T: Clone> {
    slot: PAtomic<Node<T>>,
}

impl<T: Clone> Default for Exchanger<T> {
    fn default() -> Self {
        Self {
            // 기존 논문에선 시작 slot이 Default Node임
            // 장황한 구현 및 공간 낭비의 이유로 null로 바꿈
            slot: PAtomic::null(),
        }
    }
}

impl<T: Clone> Exchanger<T> {
    fn exchange<C: ExchangeType<T>>(
        &self,
        client: &mut C,
        value: T,
        timeout: Timeout,
        cond: ExchangeCond<T>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<T, TryFail> {
        let mut myop = client.node().load(Ordering::SeqCst, guard);

        if myop.is_null() {
            // myop이 null이면 node 할당이 안 된 것이다
            let n = POwned::new(Node::from(value), pool).into_shared(guard);
            client.node().store(n, Ordering::SeqCst);
            myop = n;
        }

        let myop_ref = unsafe { myop.deref(pool) };

        let start_time = Utc::now();
        loop {
            const WAITING: usize = 0; // default
            const BUSY: usize = 1;

            // slot의 상태에 따른 case는 총 네 가지
            // - Case 1 (null)    : slot에 아무도 없음
            // - Case 2 (WAITING) : slot에서 내 node가 기다림
            // - Case 3 (WAITING) : slot에서 다른 node가 기다림
            // - Case 4 (BUSY)    : slot에서 누군가가 짝짓기 중 (나일 수도 있음)
            let yourop = self.slot.load(Ordering::SeqCst, guard);

            // 내 교환이 이미 끝났다면, 상대에게 가져온 값을 반환함
            if myop_ref.response.load(Ordering::SeqCst) {
                return Ok(Self::finish(myop_ref));
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
                            PShared::null(),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        return Err(TryFail);
                    }

                    // 누군가를 helping 함
                    let yourop = self.slot.load(Ordering::SeqCst, guard);
                    if yourop.tag() == BUSY {
                        self.help(yourop, guard, pool);
                    }

                    // helping 대상이 나일 수도 있으므로 마지막 확인
                    if myop_ref.response.load(Ordering::SeqCst) {
                        return Ok(Self::finish(myop_ref));
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
                    guard,
                );
                continue;
            }

            match yourop.tag() {
                WAITING if myop == yourop => {
                    // Case 2: slot에서 내 node가 기다림
                }
                WAITING => {
                    // Case 3: slot에서 다른 node가 기다림

                    let yourop_ref = unsafe { yourop.deref(pool) };
                    if !cond(&yourop_ref.mine) {
                        // 내가 원하는 짝이 아닐 경우 재시도
                        continue;
                    }

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
                        self.help(myop, guard, pool);
                        return Ok(Self::finish(myop_ref));
                    }
                }
                BUSY => {
                    // Case 4: slot에서 누군가가 짝짓기 중 (나일 수도 있음)
                    self.help(yourop, guard, pool);
                }
                _ => {
                    unreachable!("Tag is either WAITING or BUSY");
                }
            }
        }
    }

    /// 짝짓기 된 pair를 교환시켜 줌
    fn help(&self, yourop: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) {
        let yourop_ref = unsafe { yourop.deref(pool) };
        let partner = yourop_ref.partner.load(Ordering::SeqCst, guard);
        let partner_ref = unsafe { partner.deref(pool) };

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
            PShared::null(),
            Ordering::SeqCst,
            Ordering::SeqCst,
            guard,
        );
    }

    /// 상대에게서 받아온 item을 반환
    fn finish(myop_ref: &Node<T>) -> T {
        unsafe { (*myop_ref.yours.as_ptr()).clone() }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Exchanger<T> {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use chrono::Duration;
    use crossbeam_utils::thread;
    use serial_test::serial;

    use crate::{
        plocation::ralloc::{Collectable, GarbageCollection},
        utils::tests::{run_test, TestRootOp},
    };

    use super::*;

    /// 두 스레드가 한 exchanger를 두고 잘 교환하는지 (1회) 테스트
    struct ExchangeOnce {
        xchg: Exchanger<usize>,
        exchanges: [Exchange<usize>; 2],
    }

    impl Default for ExchangeOnce {
        fn default() -> Self {
            Self {
                xchg: Default::default(),
                exchanges: array_init::array_init(|_| Exchange::<usize>::default()),
            }
        }
    }

    impl Collectable for ExchangeOnce {
        fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
            todo!()
        }
    }

    impl Memento for ExchangeOnce {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = !;

        fn run<'o>(
            &'o mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            let xchg = &self.xchg;
            let exchanges = &mut self.exchanges;

            #[allow(box_pointers)]
            thread::scope(|scope| {
                for tid in 0..2 {
                    let exchange = unsafe {
                        (exchanges.get_unchecked_mut(tid) as *mut Exchange<usize>)
                            .as_mut()
                            .unwrap()
                    };
                    let _ = scope.spawn(move |_| {
                        // `move` for `tid`
                        let ret = exchange.run(xchg, (tid, |_| true), pool).unwrap();
                        assert_eq!(ret, 1 - tid);
                    });
                }
            })
            .unwrap();

            Ok(())
        }

        fn reset(&mut self, _: bool, _: &PoolHandle) {
            todo!("reset test")
        }
    }

    impl TestRootOp for ExchangeOnce {}

    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn exchange_once() {
        const FILE_NAME: &str = "exchange_once.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<ExchangeOnce, _>(FILE_NAME, FILE_SIZE)
    }

    /// 세 스레드가 인접한 스레드와 아이템을 교환하여 전체적으로 rotation 되는지 테스트
    ///     (exchange0)        (exchange1_0)     (exchange1_2)        (exchange2)
    /// [item0]    <-----lxchg----->       [item1]       <-----rxchg----->    [item2]
    struct RotateLeft {
        lxchg: Exchanger<usize>,
        rxchg: Exchanger<usize>,

        item0: usize,
        item1: usize,
        item2: usize,

        exchange0: Exchange<usize>,
        exchange1_0: Exchange<usize>,
        exchange1_2: Exchange<usize>,
        exchange2: Exchange<usize>,
    }

    impl Default for RotateLeft {
        fn default() -> Self {
            Self {
                lxchg: Default::default(),
                rxchg: Default::default(),
                item0: 0,
                item1: 1,
                item2: 2,
                exchange0: Default::default(),
                exchange1_0: Default::default(),
                exchange1_2: Default::default(),
                exchange2: Default::default(),
            }
        }
    }

    impl Collectable for RotateLeft {
        fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
            todo!()
        }
    }

    impl Memento for RotateLeft {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = !;

        /// Before rotation : [0]  [1]  [2]
        /// After rotation  : [1]  [2]  [0]
        fn run<'o>(
            &'o mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            let lxchg = &self.lxchg;
            let rxchg = &self.rxchg;
            let exchange0 = &mut self.exchange0;
            let exchange1_0 = &mut self.exchange1_0;
            let exchange1_2 = &mut self.exchange1_2;
            let exchange2 = &mut self.exchange2;
            let item0 = &mut self.item0;
            let item1 = &mut self.item1;
            let item2 = &mut self.item2;

            #[allow(box_pointers)]
            thread::scope(|scope| {
                let _ = scope.spawn(|_| {
                    // [0] -> [1]    [2]
                    *item0 = exchange0.run(lxchg, (*item0, |_| true), pool).unwrap();
                    assert_eq!(*item0, 1);
                });

                let _ = scope.spawn(|_| {
                    // [0]    [1] <- [2]
                    *item2 = exchange2.run(rxchg, (*item2, |_| true), pool).unwrap();
                    assert_eq!(*item2, 0);
                });

                // Composition in the middle
                // Step1: [0] <- [1]    [2]
                *item1 = exchange1_0.run(lxchg, (*item1, |_| true), pool).unwrap();
                assert_eq!(*item1, 0);

                // Step2: [1]    [0] -> [2]
                *item1 = exchange1_2.run(rxchg, (*item1, |_| true), pool).unwrap();
                assert_eq!(*item1, 2);
            })
            .unwrap();

            Ok(())
        }

        fn reset(&mut self, _: bool, _: &PoolHandle) {
            todo!("reset test")
        }
    }

    impl TestRootOp for RotateLeft {}

    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn rotate_left() {
        const FILE_NAME: &str = "rotate_left.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<RotateLeft, _>(FILE_NAME, FILE_SIZE)
    }

    const NR_THREAD: usize = 12;
    const COUNT: usize = 1_000_000;

    /// 여러 스레드가 하나의 exchanger를 두고 서로 교환하는 테스트
    /// 마지막에 서로가 교환 후 아이템 별 총 개수가 교환 전 아이템 별 총 개수와 일치하는지 체크
    struct ExchangeMany {
        xchg: Exchanger<usize>,
        exchanges: [[TryExchange<usize>; COUNT]; NR_THREAD],
    }

    impl Default for ExchangeMany {
        fn default() -> Self {
            Self {
                xchg: Default::default(),
                exchanges: array_init::array_init(|_| {
                    array_init::array_init(|_| TryExchange::<usize>::default())
                }),
            }
        }
    }

    impl Collectable for ExchangeMany {
        fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
            todo!()
        }
    }

    impl Memento for ExchangeMany {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = ();

        fn run<'o>(
            &'o mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            let xchg = &self.xchg;
            let exchanges = &mut self.exchanges;

            let unfinished = &Unfinished::default();

            #[allow(box_pointers)]
            thread::scope(|scope| {
                for tid in 0..NR_THREAD {
                    let exchanges_arr = unsafe {
                        (exchanges.get_unchecked_mut(tid) as *mut [TryExchange<usize>])
                            .as_mut()
                            .unwrap()
                    };

                    let _ = scope.spawn(move |_| {
                        // `move` for `tid`
                        for (i, exchange) in exchanges_arr.iter_mut().enumerate() {
                            if let Err(_) = exchange.run(
                                xchg,
                                (tid, Duration::milliseconds(500), |_| true),
                                pool,
                            ) {
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
                // 원인: 어떤 스레드가 자기 빼고 다 끝난 줄 알고 (긴 시간 경과) 테스트를 끝냈는데
                //      알고보니 그러지 않았던 경우 발생
                return Err(());
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
                        .run(xchg, (666, Duration::milliseconds(0), |_| true), pool)
                        .unwrap(); // 이미 끝난 op이므로 (1) dummy input은 영향 없고 (2) 반드시 리턴.
                    results[ret] += 1;
                }
            }

            // Check results
            assert!(results
                .iter()
                .enumerate()
                .all(|(tid, r)| *r == expected[tid]));

            Ok(())
        }

        fn reset(&mut self, _: bool, _: &PoolHandle) {
            todo!("reset test")
        }
    }

    impl TestRootOp for ExchangeMany {}

    /// 여럿이서 exchange 하다가 혼자만 남은 tid와 exchange한 횟수
    #[derive(Default)]
    struct Unfinished {
        flag: AtomicUsize,
        tid: AtomicUsize,
        cnt: AtomicUsize,
    }

    /// 스레드 여러 개의 exchange
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn exchange_many() {
        const FILE_NAME: &str = "exchange_many.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<ExchangeMany, _>(FILE_NAME, FILE_SIZE);
    }
}
