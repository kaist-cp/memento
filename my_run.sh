#!/bin/bash

set -e

rm -rf /mnt/pmem0/*
cargo check --tests
RUST_MIN_STACK=5073741824 cargo test --release queue_comb -- --nocapture
