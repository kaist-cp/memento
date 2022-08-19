#!/bin/bash

cargo clean

RUSTFLAGS="-Z sanitizer=address" cargo build --tests --release --features=no_persist --features=tcrash --target x86_64-unknown-linux-gnu
rm -f $SCRIPT_DIR/../../target/release/deps/memento-*.d
