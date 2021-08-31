//! Utilities

#[cfg(test)]
pub(crate) mod tests {
    use std::env;
    use std::io::Error;
    use std::path::Path;
    use tempfile::NamedTempFile;

    use crate::persistent::POp;
    use crate::plocation::pool::*;

    /// 테스트 파일이 위치할 경로 계산
    ///
    /// e.g. "foo.pool" => "{project-path}/test/foo.pool"
    pub(crate) fn get_test_path<P: AsRef<Path>>(filepath: P) -> String {
        let mut path = std::path::PathBuf::new();
        path.push(env!("CARGO_MANIFEST_DIR")); // 프로젝트 경로
        path.push("test");
        path.push(filepath);
        path.to_str().unwrap().to_string()
    }

    /// test에 사용하기 위한 더미용 PoolHandle 얻기
    pub(crate) fn get_test_handle() -> Result<PoolHandle, Error> {
        #[derive(Default)]
        struct RootOp {}
        impl POp for RootOp {
            type Object = ();
            type Input = ();
            type Output = Result<(), ()>;
            fn run(&mut self, _: &Self::Object, _: Self::Input, _: &PoolHandle) -> Self::Output {
                Ok(())
            }
            fn reset(&mut self, _: bool) {
                // no-op
            }
        }

        // 임시파일 경로 얻기. `create`에서 파일이 이미 존재하면 실패하기 때문에 여기선 경로만 얻어야함
        let temp_path = NamedTempFile::new()?
            .path()
            .to_str()
            .unwrap()
            .to_owned()
            .clone();

        // 할당 많이하는 테스트를 대비해 8GB로 생성
        Pool::create::<RootOp>(&temp_path, 8 * 1024 * 1024 * 1024)
    }
}
