//! Single-threaded counter

// TODO(pmem 사용(#31, #32)):
// - persist를 위해 flush/fence 추가
// - persistent location 위에서 동작

use crate::persistent::*;

/// `Counter`의 `fetch_add()` 도중의 상태들
#[derive(Debug)]
enum State {
    Fetching,
    Adding,
    Finished,
}

/// `Counter`의 `fetch_add()`를 호출할 때 쓰일 client
#[derive(Debug)]
pub struct FetchAddClient {
    output: usize,
    state: State,
    // state와 output을 합치는 디자인도 고려해볼 수 있지만
    // persistency atomicity 때문에 state가 output 저장 완료 flag를 담당해야 함
}

impl Default for FetchAddClient {
    fn default() -> Self {
        Self {
            output: 0,
            state: State::Fetching,
        }
    }
}

impl PersistentClient for FetchAddClient {
    fn reset(&mut self) {
        self.state = State::Fetching;
    }
}

/// `Counter`의 `fetch()`를 호출할 때 쓰일 client
#[derive(Debug)]
pub struct FetchClient {
    output: usize,
    response: bool,
}

impl Default for FetchClient {
    fn default() -> Self {
        Self {
            output: 0,
            response: false,
        }
    }
}

impl PersistentClient for FetchClient {
    fn reset(&mut self) {
        self.response = false;
    }
}

/// 싱글스레드 카운터
///
/// 정수를 증가시키거나 읽음
#[derive(Debug)]
pub struct Counter {
    n: usize,
}

impl Counter {
    /// init으로 Counter 초기화
    pub fn new(init: usize) -> Self {
        Self { n: init }
    }

    /// 현재 정수에 val을 더하고 더하기 전 값을 반환함
    pub fn fetch_add(&mut self, client: &mut FetchAddClient, val: usize) -> usize {
        loop {
            match client.state {
                State::Fetching => {
                    client.output = self.n;
                    client.state = State::Adding; // TODO: 컴파일러 최적화에 의해 생략되지 않는지 확인
                }
                State::Adding => {
                    self.n = client.output + val;
                    client.state = State::Finished;
                }
                State::Finished => break,
            }
        }

        client.output
    }

    /// 현재 정수를 반환함
    pub fn fetch(&self, client: &mut FetchClient) -> usize {
        if !client.response {
            client.output = self.n;
            client.response = true;
        }

        client.output
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const COUNT: usize = 1_000_000;

    /// Counter에 1을 한 번만 더하는 테스트
    /// 같은 client로 여러 번 `fetch_add()`해도 최종적으로 딱 1만 더해짐
    #[test]
    fn add_1_once() {
        let mut cnt = Counter::new(0); // TODO(persistent location)
        let mut faa_client = FetchAddClient::default(); // TODO(persistent location)

        // 아래 로직은 idempotent 함
        for _ in 0..COUNT {
            // same faa client w/o reset() -> same faa result
            let ret = cnt.fetch_add(&mut faa_client, 1);
            assert_eq!(ret, 0);
        }
    }

    /// Counter에 1을 여러 번 더하는 테스트
    #[test]
    fn add_1_n_times() {
        let mut cnt = Counter::new(0); // TODO(persistent location)
        let mut faa_client = FetchAddClient::default(); // TODO(persistent location)
        let mut f_client = FetchClient::default(); // TODO(persistent location)
        let mut i = 0; // TODO(persistent location)

        // 아래 로직은 idempotent 함
        // same faa client w/ reset() -> different faa result
        while i < COUNT {
            let _ = cnt.fetch_add(&mut faa_client, 1);
            faa_client.reset();
            i += 1;
        }

        let ret1 = cnt.fetch(&mut f_client);
        assert_eq!(ret1, COUNT);

        // same f client -> same f result
        let ret2 = cnt.fetch(&mut f_client);
        assert_eq!(ret1, ret2);
    }
}
