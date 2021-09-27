//! Utilities

#[doc(hidden)]
pub mod tests {
    use std::env;
    use std::io::Error;
    use std::path::Path;
    use tempfile::NamedTempFile;

    use crate::persistent::POp;
    use crate::plocation::pool::*;

    /// 테스트 파일이 위치할 경로 계산
    ///
    /// e.g. "foo.pool" => "{project-path}/test/foo.pool"
    pub fn get_test_path<P: AsRef<Path>>(filepath: P) -> String {
        let mut path = std::path::PathBuf::new();
        path.push(env!("CARGO_MANIFEST_DIR")); // 프로젝트 경로
        path.push("test");
        path.push(filepath);
        path.to_str().unwrap().to_string()
    }

    #[derive(Debug, Default)]
    pub struct TestRootOp {}
    impl POp for TestRootOp {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = Result<(), ()>;

        fn run<'o, O: POp>(
            &mut self,
            _: Self::Object<'o>,
            _: Self::Input,
            _: &PoolHandle<O>,
        ) -> Self::Output<'o> {
            Ok(())
        }
        fn reset(&mut self, _: bool) {
            // no-op
        }
    }

    /// test에 사용하기 위한 더미용 PoolHandle 얻기
    pub fn get_test_handle(filesize: usize) -> Result<PoolHandle<TestRootOp>, Error> {
        // 임시파일 경로 얻기. `create`에서 파일이 이미 존재하면 실패하기 때문에 여기선 경로만 얻어야함
        let temp_path = NamedTempFile::new()?.path().to_str().unwrap().to_owned();
        // 풀 생성 및 핸들 반환
        Pool::create::<TestRootOp>(&temp_path, filesize)
    }
}
