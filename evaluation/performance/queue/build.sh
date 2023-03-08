#!/bin/bash

set -e
base_dir=$(dirname "$0")
dir_path=$(dirname $(realpath $0))
target_path=$dir_path/target/release
mkdir -p $target_path

feature=$1

# Install dependency

### Clobber-NVM
sudo $base_dir/src/clobber-nvm/deps.sh

# Build

### PMDK
g++ -O3 -o $target_path/bench_cpp $base_dir/src/main.cpp $base_dir/src/pmdk/pipe.cpp $base_dir/src/pmdk/queue.cpp -pthread -lpmemobj -std=c++17

### Clobber-NVM
(cd $base_dir/src/clobber-nvm; sudo ./build.sh)
(cd $base_dir/src/clobber-nvm/apps/queue/; make benchmark-clobber)

### Rust implementations
cd $dir_path
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
cargo build --release
