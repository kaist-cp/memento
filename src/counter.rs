use std::sync::atomic::{fence, Ordering};

use crate::persistent::*;

#[derive(Debug)]
enum State {
    Start,
    Modifying,
    End,
}

macro_rules! make_info {
    ($info:ident) => {
        /// Counter의 op을 호출할 때 쓰일 info
        // FetchAdd, Fetch 모두 같은 info가 쓰이므로 macro 처리
        #[derive(Debug)]
        pub struct $info {
            output: i32,
            state: State,
        }

        impl Default for $info {
            fn default() -> Self {
                Self {
                    output: 0,
                    state: State::Start,
                }
            }
        }

        impl PersistentInfo for $info {
            fn reset(&mut self) {
                self.state = State::Start;
            }
        }
    };
}

make_info!(FetchAddInfo);
make_info!(FetchInfo);

/// 싱글스레드 카운터
///
/// 정수를 증가시키거나 읽음
#[derive(Debug)]
pub struct Counter {
    n: i32,
}

impl Counter {
    /// init으로 Counter 초기화
    pub fn new(init: i32) -> Self {
        Self { n: init }
    }

    /// 현재 정수에 val을 더하고 더하기 전 값을 반환함
    pub fn fetch_add(&mut self, info: &mut FetchAddInfo, val: i32) -> i32 {
        loop {
            match info.state {
                State::Start => {
                    info.output = self.n;
                    fence(Ordering::SeqCst);
                    info.state = State::Modifying; // TODO: 컴파일러 최적화에 의해 생략되지 않는지 확인
                }
                State::Modifying => {
                    self.n = info.output + val;
                    fence(Ordering::SeqCst);
                    info.state = State::End;
                }
                State::End => break,
            }
        }

        info.output
    }

    /// 현재 정수를 반환함
    pub fn fetch(&self, info: &mut FetchInfo) -> i32 {
        loop {
            match info.state {
                State::Start => {
                    info.output = self.n;
                    fence(Ordering::SeqCst);
                    info.state = State::End;
                }
                State::End => break,
                _ => unreachable!(),
            }
        }

        info.output
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const COUNT: usize = 1_000_000;

    /// Counter에 1을 한 번만 더하는 테스트
    /// 같은 info로 여러 번 fetch_add()해도 최종적으로 딱 1만 더해짐
    #[test]
    fn add_1_once() {
        let mut cnter = Counter::new(0);
        let mut faa_info = FetchAddInfo::default();

        // ↑ 위 변수들이 persistent 하다면
        // ↓ 아래 로직은 idempotent 함

        for _ in 0..COUNT {
            // same faa info w/o reset() -> same faa result
            let ret = cnter.fetch_add(&mut faa_info, 1);
            assert_eq!(ret, 0);
        }
    }

    /// Counter에 1을 여러 번 더하는 테스트
    #[test]
    fn add_1_n_times() {
        let mut cnter = Counter::new(0);
        let mut faa_info = FetchAddInfo::default();
        let mut f_info = FetchInfo::default();
        let mut i = 0;

        // ↑ 위 변수들이 persistent 하다면
        // ↓ 아래 로직은 idempotent 함

        // same faa info w/ reset() -> different faa result
        while i < COUNT {
            let _ = cnter.fetch_add(&mut faa_info, 1);
            faa_info.reset();
            fence(Ordering::SeqCst);
            i += 1;
        }

        let ret1 = cnter.fetch(&mut f_info);
        assert_eq!(ret1, COUNT as i32);

        // same f info -> same f result
        let ret2 = cnter.fetch(&mut f_info);
        assert_eq!(ret1, ret2);
    }
}
