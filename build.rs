use std::process::Command;

fn main() {
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

    #[cfg(feature = "pmcheck")]
    {
        //     // TODO: Set pmcheck bin path
        //     println!("cargo:rustc-link-search=/home/ubuntu/seungmin.jeon/pldi2023-rebuttal/psan-myself/pmcheck/bin");
        //     println!("cargo:rustc-link-lib=pmcheck");
        println!("cargo:rustc-link-lib=pmemobj");
    }
}
