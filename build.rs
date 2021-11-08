use std::process::Command;

const RALLOC_REPO: &str = "ssh://git@cp-git.kaist.ac.kr:9001/persistent-mem/ralloc.git";
const RALLOC_BRANCH: &str = "wo_gc"; // ci 통과를 위해 ci가 사용할 최신 버전 branch

fn main() {
    // Clone and checkout to latest branch
    Command::new("git")
        .args(&["clone", RALLOC_REPO])
        .current_dir("./ext")
        .status()
        .expect("failed to git clone!");
    Command::new("git")
        .args(&["checkout", RALLOC_BRANCH])
        .current_dir("./ext/ralloc")
        .status()
        .expect("failed to git checkout!");

    // Build libralloc.a
    let args = {
        #[cfg(not(feature = "no_persist"))]
        {
            &["libralloc.a"]
        }
        #[cfg(feature = "no_persist")]
        {
            &["libralloc.a", "FEATURE=no_persist"]
        }
    };
    Command::new("make")
        .args(args)
        .current_dir("./ext/ralloc/test")
        .status()
        .expect("failed to make!");

    // Link libralloc.a
    println!("cargo:rustc-link-search=ext/ralloc/test");
    println!("cargo:rustc-link-lib=dylib=stdc++");
}
