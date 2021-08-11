//! Utilities

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
