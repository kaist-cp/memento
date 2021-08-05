//! Persistent Stack implemented using Array

use crate::persistent::*;

const CAPACITY: usize = 5;

/// 배열로 동작하는 고정크기 스택
#[derive(Debug)]
pub struct ArrayStack {
    array: [usize; CAPACITY],
    top: usize, // 원소 넣을 자리를 가리키는 top 인덱스
}

impl Default for ArrayStack {
    fn default() -> Self {
        Self {
            // usize::MAX은 "empty"를 의미
            array: [usize::MAX; CAPACITY],
            top: 0,
        }
    }
}

impl ArrayStack {
    fn is_full(&self) -> bool {
        self.top == CAPACITY
    }

    /// top이 가리키는 자리에 원소 넣고 top+=1
    fn push(&mut self, client: &mut PushClient, val: usize) -> Result<(), String> {
        if self.is_full() {
            return Err("there is no more space".to_string());
        }

        // # Phase "Output 확인"
        match client.output {
            // Output 있으면 끝난 Op임. output만 도로 뱉으면됨
            Output::Some(o) => return Ok(o),
            // Output 없으면 끝난 Op 아님. output 만들어야함
            Output::None => {}
        }

        // # Phase "Output 생성"
        // input 등록도 안된 상태면 input부터 등록
        if !client.has_input() {
            client.set_input(self.top);
        }
        // 할 거 다하기 전에(commit point 전에) 터졌으면 다시함
        let client_input = client.get_input().unwrap();
        if client_input == self.top {
            self.array[client_input] = val;
            self.top += 1; // "할 거 다했음" commit point
        }
        // output 등록 후 마무리
        client.output = Output::Some(());
        Ok(())
    }

    /// index의 값 반환
    pub fn get(&self, index: usize) -> usize {
        self.array[index]
    }

    /// 상태 출력
    pub fn print_state(&self, msg: &str) {
        println!("{}", msg);
        for (ix, val) in self.array.iter().enumerate() {
            println!("[{}]: {}", ix, val);
        }
        println!("top: {}", self.top);
    }
}

impl PersistentOpMut<PushClient> for ArrayStack {
    type Input = usize;
    type Output = Result<(), String>;

    fn persistent_op_mut(&mut self, client: &mut PushClient, input: Self::Input) -> Self::Output {
        self.push(client, input)
    }
}

/// TODO doc
#[derive(Debug, PartialEq)]
enum Input<T> {
    /// 초기 상태
    None,

    /// TODO doc
    Some(T),
}

/// TODO doc
#[derive(Debug, PartialEq)]
enum Output<T> {
    /// TODO doc
    None,

    /// TODO doc
    Some(T),
}

/// dd
#[derive(Debug)]
pub struct PushClient {
    /// None: 초기 상태
    /// Some(usize): 준비된 상태 (가리키는 usize index에 value 넣을 준비)
    index: Input<usize>,

    output: Output<()>,
}

impl PushClient {
    fn has_input(&self) -> bool {
        self.index != Input::None
    }

    fn set_input(&mut self, index: usize) {
        self.index = Input::Some(index);
    }

    fn get_input(&self) -> Result<usize, ()> {
        if let Input::Some(index) = self.index {
            return Ok(index);
        }
        Err(())
    }
}

impl Default for PushClient {
    fn default() -> Self {
        Self {
            index: Input::None,
            output: Output::None,
        }
    }
}

impl PersistentClient for PushClient {
    fn reset(&mut self) {
        self.index = Input::None;
        self.output = Output::None;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::plocation::pool::*;

    struct RootObj {
        stack: ArrayStack,
    }

    impl RootObj {
        // idempotent: 이 함수를 몇번 실행하든 첫 2개만 push됨
        #[allow(warnings)]
        fn run(&mut self, root_client: &mut RootClient, _input: ()) -> Result<(), ()> {
            self.stack.print_state("----- Before push -----");
            for _ in 1..10 {
                self.stack.push(&mut root_client.push_client0, 0).unwrap();
                self.stack.push(&mut root_client.push_client1, 1).unwrap();
            }
            self.stack.print_state("\n----- After push -----");

            // 첫 2개만 push 되고 나머지 자리는 empty인지 확인 (usize::MAX가 empty를 의미)
            assert_eq!(self.stack.get(0), 0);
            assert_eq!(self.stack.get(1), 1);
            for ix in 2..CAPACITY {
                assert_eq!(self.stack.get(ix), usize::MAX);
            }
            Ok(())
        }
    }

    impl PersistentOpMut<RootClient> for RootObj {
        type Input = ();
        type Output = Result<(), ()>;

        fn persistent_op_mut(
            &mut self,
            client: &mut RootClient,
            input: Self::Input,
        ) -> Self::Output {
            self.run(client, input)
        }
    }

    struct RootClient {
        push_client0: PushClient,
        push_client1: PushClient,
    }

    impl Default for RootClient {
        fn default() -> Self {
            Self {
                push_client0: PushClient::default(),
                push_client1: PushClient::default(),
            }
        }
    }

    impl PersistentClient for RootClient {
        fn reset(&mut self) {
            unimplemented!();
        }
    }

    const FILE_NAME: &str = "test/array_stack.pool";
    const FILE_SIZE: usize = 8 * 1024;

    #[test]
    fn push_2_times() {
        // 풀 새로 만들기를 시도. 새로 만들기를 성공했다면 true
        let is_new_file = Pool::create::<RootObj, RootClient>(FILE_NAME, FILE_SIZE).is_ok();

        // 풀 열기
        let pool_handle = Pool::open(FILE_NAME).unwrap();
        let mut root_ptr = pool_handle.get_root::<RootObj, RootClient>().unwrap();
        let (root_obj, root_client) = unsafe { root_ptr.deref_mut() };

        // 새로 만든 풀이라면 루트 오브젝트 초기화
        if is_new_file {
            *root_obj = RootObj {
                stack: ArrayStack::default(),
            };
        }
        root_obj.persistent_op_mut(root_client, ()).unwrap();
    }
}
