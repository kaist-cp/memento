#!/bin/bash

set -e
dir_path=$(dirname $(realpath $0))
target_path=$dir_path/target/release
mkdir -p $target_path

# TODO? install libndctl-dev?

# Install dependency for plot
# sudo apt install build-essential python3-pip
# pip3 install --user pandas matplotlib

# Build Rust implementation
cargo build --release;

# Build C++ implementation
# src/pmdk/build.sh # Install PMDK
g++ -o $target_path/bench_cpp src/main.cpp src/pmdk/pipe.cpp -pthread -lpmemobj -std=c++17 # TODO: Corundum은 왜 -O2 사용?
