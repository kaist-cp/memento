//! Utilities

use crate::persistent::{PersistentClient, PersistentOp};
use crate::plocation::pool::{Pool, PoolHandle};

/// 테스트 파일이 위치할 경로 계산
///
/// e.g. "foo.pool" => "{project-path}/test/foo.pool"
pub fn get_test_path(filename: &str) -> String {
    let mut path = std::path::PathBuf::new();
    path.push(env!("CARGO_MANIFEST_DIR")); // 프로젝트 경로
    path.push("test");
    path.push(filename);
    path.to_str().unwrap().to_string()
}

/// test에 사용하기 위한 더미용 PoolHandle 얻기
// NOTE: 다른 test에 같은 파일이름 사용하면 안됨. test가 parallel하게 돔
pub fn get_test_handle(filename: &str) -> PoolHandle {
    #[derive(Default)]
    struct MyRootObj {}

    impl MyRootObj {
        fn my_root_op(&self, _: &mut MyRootClient, _: ()) -> Result<(), ()> {
            Ok(())
        }
    }

    impl PersistentOp<MyRootClient> for MyRootObj {
        type Input = ();
        type Output = Result<(), ()>;

        fn persistent_op(&self, client: &mut MyRootClient, input: Self::Input) -> Self::Output {
            self.my_root_op(client, input)
        }
    }

    #[derive(Default)]
    struct MyRootClient {}

    impl PersistentClient for MyRootClient {
        fn reset(&mut self) {}
    }

    let filepath = get_test_path(filename);
    let _ = std::fs::remove_file(&filepath); // 기존 파일 제거
    Pool::create::<MyRootObj, MyRootClient>(&filepath, 8 * 1024).unwrap()
}
