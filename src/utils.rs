//! Utilities

#[doc(hidden)]
pub mod tests {
    use std::env;
    use std::io::Error;
    use std::path::Path;
    use tempfile::NamedTempFile;

    use crate::persistent::POp;
    use crate::plocation::pool::*;
    use crate::plocation::ralloc::Collectable;

    /// 테스트 파일이 위치할 경로 계산
    ///
    /// e.g. "foo.pool" => "{project-path}/test/foo.pool"
    pub fn get_test_abs_path<P: AsRef<Path>>(rel_path: P) -> String {
        let mut path = std::path::PathBuf::new();
        path.push(env!("CARGO_MANIFEST_DIR")); // 프로젝트 경로
        path.push("test");
        path.push(rel_path);
        path.to_str().unwrap().to_string()
    }

    #[derive(Debug, Default)]
    pub struct DummyRootOp;

    impl Collectable for DummyRootOp {
        unsafe extern "C" fn filter(_: *mut std::os::raw::c_char, _: *mut crate::plocation::ralloc::GarbageCollection) {
            // no-op
        }
    }

    impl POp for DummyRootOp {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = !;

        fn run<'o, O: POp>(
            &mut self,
            _: Self::Object<'o>,
            _: Self::Input,
            _: &PoolHandle<O>,
        ) -> Result<Self::Output<'o>, Self::Error> {
            Ok(())
        }
        fn reset(&mut self, _: bool) {
            // no-op
        }
    }

    /// test에 사용하기 위한 더미용 PoolHandle 얻기
    pub fn get_dummy_handle(filesize: usize) -> Result<PoolHandle<DummyRootOp>, Error> {
        // 임시파일 경로 얻기. `create`에서 파일이 이미 존재하면 실패하기 때문에 여기선 경로만 얻어야함
        let temp_path = NamedTempFile::new()?.path().to_str().unwrap().to_owned();
        // 풀 생성 및 핸들 반환
        Pool::create::<DummyRootOp>(&temp_path, filesize)
    }

    /// test를 위한 root op은 아래 조건을 만족하자
    pub trait TestRootOp: for<'o> POp<Object<'o> = (), Input = ()> {}

    /// test op 돌리기
    pub fn run_test<O: TestRootOp, P: AsRef<Path>>(pool_name: P, pool_len: usize) {
        let filepath = get_test_abs_path(pool_name);

        // 풀 열기 (없으면 새로 만듦)
        let pool_handle = unsafe { Pool::open(&filepath, pool_len) }
            .unwrap_or_else(|_| Pool::create::<O>(&filepath, pool_len).unwrap());

        // 루트 op 가져오기
        let root_op = pool_handle.get_root();

        // 루트 op 실행
        while root_op.run((), (), &pool_handle).is_err() {}
    }
}
