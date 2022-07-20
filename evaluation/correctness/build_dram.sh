#!/bin/bash

# PMEM_PATH="/mnt/pmem0"

SCRIPT_DIR=`dirname $(realpath "$0")`
OUT_PATH="$SCRIPT_DIR/out"
# mkdir -p $OUT_PATH
# mkdir -p $PMEM_PATH/test
cargo clean

cargo build --tests --release --features=no_persist
rm -f $SCRIPT_DIR/../../target/release/deps/memento-*.d
