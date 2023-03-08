use std::process::Command;

fn build_pmdk() {
    Command::new("git")
        .args(["submodule", "update", "--init", "--recursive"])
        .current_dir("./")
        .status()
        .expect("failed to submodule update!");

    Command::new("git")
        .args(["apply", "../pmdk-rs.patch"])
        .current_dir("./ext/pmdk-rs")
        .status()
        .expect("failed to submodule update!");

    println!("cargo:rustc-link-lib=pmemobj");

}

fn build_ralloc() {
    // Build Ralloc
    Command::new("make")
        .args(["clean"])
        .current_dir("./ext/ralloc/test")
        .status()
        .expect("failed to make clean!");
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

fn main() {
    build_ralloc();
    build_pmdk();
}
