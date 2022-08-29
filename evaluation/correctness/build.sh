#!/bin/bash
feature=$1
SCRIPT_DIR=`dirname $(realpath "$0")`

# Build
cargo clean
if [ "$feature" == "no_persist" ]; then
    RUSTFLAGS="-Z sanitizer=address" cargo build --tests --release --features=no_persist --features=tcrash --target x86_64-unknown-linux-gnu
else
    cargo build --tests --release --features=tcrash
fi
rm -f $SCRIPT_DIR/../../target/release/deps/memento-*.d
