#!/bin/bash

set -e
base_dir=$(dirname "$0")
dir_path=$(dirname $(realpath $0))
target_path=$dir_path/target/release
mkdir -p $target_path

feature=$1

# Install dependency

### PMDK
sudo apt install libpmemobj1 -y
sudo apt install libpmemobj-cpp-dev -y

### Clobber-NVM
sudo ./src/clobber-nvm/deps.sh

### plot
sudo apt install build-essential python3-pip
pip3 install --user pandas matplotlib gitpython

# Build

### Rust implementation
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
if [ "$feature" == "no_persist" ]; then
    cargo build --release --features $feature
else
    cargo build --release
fi

### PMDK
g++ -O3 -o $target_path/bench_cpp $base_dir/src/main.cpp $base_dir/src/pmdk/pipe.cpp $base_dir/src/pmdk/queue.cpp -pthread -lpmemobj -std=c++17

### Clobber-NVM
(cd ./src/clobber-nvm; sudo ./build.sh)
(cd ./src/clobber-nvm/apps/queue/; make benchmark-clobber)
