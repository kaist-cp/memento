//! Utilities

#[doc(hidden)]
pub mod tests {
    use crossbeam_epoch::Guard;
    use std::io::Error;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::NamedTempFile;

    use crate::persistent::{Memento, PDefault};
    use crate::plocation::pool::*;
    use crate::plocation::ralloc::{Collectable, GarbageCollection};

    /// 테스트 파일이 위치할 경로 계산
    ///
    /// e.g. "foo.pool" => "{project-path}/test/foo.pool"
    pub fn get_test_abs_path<P: AsRef<Path>>(rel_path: P) -> String {
        let mut path = std::path::PathBuf::new();
        #[cfg(not(feature = "no_persist"))]
        {
            path.push("/mnt/pmem0")
        }
        #[cfg(feature = "no_persist")]
        {
            path.push(env!("CARGO_MANIFEST_DIR")); // 프로젝트 경로
        }
        path.push("test");
        path.push(rel_path);
        path.to_str().unwrap().to_string()
    }

    #[derive(Debug)]
    pub struct DummyRootObj;

    impl Collectable for DummyRootObj {
        fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
            // no-op
        }
    }

    impl PDefault for DummyRootObj {
        fn pdefault(_: &'static PoolHandle) -> Self {
            Self {}
        }
    }

    impl TestRootObj for DummyRootObj {}

    #[derive(Debug, Default)]
    pub struct DummyRootMemento;

    impl Collectable for DummyRootMemento {
        fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
            // no-op
        }
    }

    impl Memento for DummyRootMemento {
        type Object<'o> = &'o DummyRootObj;
        type Input = usize;
        type Output<'o> = ();
        type Error = !;

        fn run<'o>(
            &'o mut self,
            _: Self::Object<'o>,
            _: Self::Input,
            _: &mut Guard,
            _: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            Ok(())
        }

        fn reset(&mut self, _: bool, _: &mut Guard, _: &'static PoolHandle) {
            // no-op
        }
    }

    /// test에 사용하기 위한 더미용 PoolHandle 얻기
    pub fn get_dummy_handle(filesize: usize) -> Result<&'static PoolHandle, Error> {
        #[cfg(not(feature = "no_persist"))]
        {
            // 임시파일 경로 얻기. `create`에서 파일이 이미 존재하면 실패하기 때문에 여기선 경로만 얻어야함
            let temp_path = NamedTempFile::new_in("/mnt/pmem0")?
                .path()
                .to_str()
                .unwrap()
                .to_owned();

            // 풀 생성 및 핸들 반환
            Pool::create::<DummyRootObj, DummyRootMemento>(&temp_path, filesize, 0)
        }
        #[cfg(feature = "no_persist")]
        {
            // 임시파일 경로 얻기. `create`에서 파일이 이미 존재하면 실패하기 때문에 여기선 경로만 얻어야함
            let temp_path = NamedTempFile::new()?.path().to_str().unwrap().to_owned();

            // 풀 생성 및 핸들 반환
            Pool::create::<DummyRootObj, DummyRootMemento>(&temp_path, filesize, 0)
        }
    }

    impl TestRootMemento<DummyRootObj> for DummyRootMemento {}

    /// test를 위한 root obj, root op은 아래 조건을 만족하자
    pub trait TestRootObj: PDefault + Collectable {}
    pub trait TestRootMemento<O: TestRootObj>:
        for<'o> Memento<Object<'o> = &'o O, Input = usize>
    {
    }

    use lazy_static::lazy_static;

    lazy_static! {
        pub static ref JOB_FINISHED: AtomicUsize = AtomicUsize::new(0);
        pub static ref RESULTS: [AtomicUsize; 1024] =
            array_init::array_init(|_| AtomicUsize::new(0));
    }

    /// test op 돌리기
    pub fn run_test<O, M, P>(pool_name: P, pool_len: usize, nr_memento: usize)
    where
        O: TestRootObj + Send + Sync,
        M: TestRootMemento<O> + Send + Sync,
        P: AsRef<Path>,
    {
        // 테스트 변수 초기화
        JOB_FINISHED.store(0, Ordering::SeqCst);
        for res in RESULTS.as_ref() {
            res.store(0, Ordering::SeqCst);
        }

        // 풀 열기 (없으면 새로 만듦)
        let filepath = get_test_abs_path(pool_name);
        let pool_handle = unsafe { Pool::open::<O, M>(&filepath, pool_len) }
            .unwrap_or_else(|_| Pool::create::<O, M>(&filepath, pool_len, nr_memento).unwrap());

        // 루트 op 실행
        pool_handle.execute::<O, M>();
    }
}
