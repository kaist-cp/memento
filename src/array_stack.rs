//! Persistent Stack implemented using Array

use crate::persistent::*;

const CAPACITY: usize = 5;

/// 배열로 동작하는 고정크기 스택 (TODO: allocator 구현 후 크기 유동적일 수 있도록)
#[derive(Debug)]
pub struct ArrayStack {
    array: [usize; CAPACITY],
    top: usize, // 원소 넣을 자리를 가리키는 인덱스
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

        // (1) client의 output을 통해 Op 상태 확인. output이 나왔었으면 여기서 끝남
        match client.output {
            // Output 없으면 끝난 Op 아님. 실행해야함
            Output::None => {}

            // Output 있으면 끝난 Op임. output만 도로 뱉으면됨
            Output::Some(o) => return Ok(o),
            // // Output이 나온데다가 다른 곳으로 소비된 상태이면 그저 pass함
            // Output::Used => return Ok(()),
        }

        // (2) output이 안나왔었으면 output을 만듦
        // input 등록도 안된 상태면 input부터 등록
        if !client.has_input() {
            // TODO 생각
            // - top이 input은 아닌데 idempotent하려면 top을 이용해야함
            // - 함수 이름을 바꿀까? 아니면 다른방법 있나?
            client.set_input(self.top);
        }

        let client_input = client.get_input().unwrap();
        // commit point까지 실행됐는지 확인
        if client_input != self.top {
            // commit point까지 실행됐으면 output만 등록해주고 끝냄
            client.output = Output::Some(());
            return Ok(());
        }
        self.array[client_input] = val;
        self.top += 1; // commit point

        client.output = Output::Some(());
        Ok(())
    }

    /// 상태 출력
    pub fn print_state(&self) {
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

// impl PersistentOpMut<PopClient> for ArrayStack {
//     type Input = ();
//     type Output = Result<usize, String>;

//     fn persistent_op_mut(&mut self, client: &mut PopClient, _input: Self::Input) -> Self::Output {
//         self.pop(client)
//     }
// }

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
    // /// TODO doc
    // Used,
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
    // input 등록된 상태인지 확인
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
    use crate::persistent::{PersistentClient, PersistentOpMut};
    use crate::plocation::pool::*;

    use super::ArrayStack;
    use super::PushClient;
    struct RootObj {
        array: ArrayStack,
    }

    impl RootObj {
        // idempotent: 이 함수를 몇번 실행하든 첫 2개만 push됨
        //
        // # 풀 처음 만들고 처음 실행했을 때 출력
        // ----- Before push -----
        // [0]: 18446744073709551615
        // [1]: 18446744073709551615
        // [2]: 18446744073709551615
        // [3]: 18446744073709551615
        // [4]: 18446744073709551615
        // top: 0
        //
        // ----- After push -----
        // [0]: 1
        // [1]: 2
        // [2]: 18446744073709551615
        // [3]: 18446744073709551615
        // [4]: 18446744073709551615
        // top: 2
        //
        // # 두 번째 실행부터는 전부 이렇게 출력
        // ----- Before push -----
        // [0]: 1
        // [1]: 2
        // [2]: 18446744073709551615
        // [3]: 18446744073709551615
        // [4]: 18446744073709551615
        // top: 2
        //
        // ----- After push -----
        // [0]: 1
        // [1]: 2
        // [2]: 18446744073709551615
        // [3]: 18446744073709551615
        // [4]: 18446744073709551615
        // top: 2
        fn run(&mut self, root_client: &mut RootClient, _input: ()) -> Result<(), ()> {
            println!("----- Before push -----");
            self.array.print_state();

            for _ in 1..10 {
                self.array.push(&mut root_client.push_client1, 1).unwrap();
                self.array.push(&mut root_client.push_client2, 2).unwrap();
            }

            println!("\n----- After push -----");
            self.array.print_state();
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
        push_client1: PushClient,
        push_client2: PushClient,
    }

    impl Default for RootClient {
        fn default() -> Self {
            Self {
                push_client1: PushClient::default(),
                push_client2: PushClient::default(),
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
    fn array_push() {
        // 풀 새로 만들기를 시도. 새로 만들기를 성공했다면 true
        let is_new_file = Pool::create::<RootObj, RootClient>(FILE_NAME, FILE_SIZE).is_ok();

        // 풀 열기
        let pool_handle = Pool::open(FILE_NAME).unwrap();
        let mut root_ptr = pool_handle.get_root::<RootObj, RootClient>().unwrap();
        let (root_obj, root_client) = unsafe { root_ptr.deref_mut() };

        // 새로 만든 풀이라면 루트 오브젝트 초기화
        if is_new_file {
            // TODO: 여기서 루트 오브젝트 초기화하기 전에 터지면 문제 발생
            // - 문제: 다시 열었을 때 루트 오브젝트를 (1) 다시 초기화해야하는지 (2) 초기화가 잘 됐는지 구분 힘듦
            // - 방안: 풀의 메타데이터 초기화할때 같이 초기화하고, 초기화가 잘 되었는지 나타내는 플래그 사용
            println!("init root");
            *root_obj = RootObj {
                array: ArrayStack::default(),
            };
        }

        root_obj.persistent_op_mut(root_client, ()).unwrap();
    }
}
