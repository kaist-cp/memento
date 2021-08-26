//! Utilities

#[cfg(test)]
pub(crate) mod test {
    use std::io::Error;
    use std::path::Path;
    use tempfile::NamedTempFile;

    use crate::persistent::PersistentOp;
    use crate::plocation::pool::{Pool, PoolHandle};

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
        struct RootObj {}

        #[derive(Default)]
        struct RootClient {}

        impl PersistentOp for RootClient {
            type Object = ();
            type Input = ();
            type Output = Result<(), ()>;

            fn run(&mut self, _: &Self::Object, _: Self::Input) -> Self::Output {
                Ok(())
            }

            fn reset(&mut self, _: bool) {}
        }

        let temp_file = NamedTempFile::new_in(get_test_path(""))?; // 테스트 폴더에 임시파일 생성
        let _ = temp_file.as_file().set_len(8 * 1024 * 1024 * 1024); // 임시파일 크기 설정. 할당 많이하는 테스트를 대비해 8GB로 함
        Pool::open(temp_file.path())
    }
}
