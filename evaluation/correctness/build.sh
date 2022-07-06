#!/bin/bash

PMEM_PATH="/mnt/pmem0"

make -j
SCRIPT_DIR=`dirname $(realpath "$0")`
OUT_PATH="$SCRIPT_DIR/out"
rm -rf $OUT_PATH
mkdir -p $OUT_PATH
mkdir -p $PMEM_PATH/test
cargo clean

cargo build --tests --release --features=simulate_tcrash
rm -f $SCRIPT_DIR/../../target/release/deps/memento-*.d