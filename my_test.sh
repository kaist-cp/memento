#!/bin/bash

# TODO: 이 파일은 프로젝트에 필요없음. 테스트 용도

set -e

rm -rf /mnt/pmem0/*
cargo check --tests
RUST_MIN_STACK=5073741824 cargo test --release queue_comb -- --nocapture
