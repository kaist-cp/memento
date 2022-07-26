#!/bin/bash

cargo clean

cargo build --tests --release --features=no_persist
rm -f $SCRIPT_DIR/../../target/release/deps/memento-*.d
