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

### plot
sudo apt install build-essential python3-pip
pip3 install --user pandas matplotlib gitpython

# Build Rust implementation
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
if [ "$feature" == "no_persist" ]; then
    cargo build --release --features $feature
else
    cargo build --release
fi

# Build C++ implementation
### TODO: Corundum은 PMDK 컴파일시 왜 -O2 사용?
### TODO: PMDK도 no_persist 같은 옵션 있는지 확인하고 있다면 적용 (참고: https://pmem.io/2015/06/12/pmem-model.html)
g++ -O3 -o $target_path/bench_cpp $base_dir/src/main.cpp $base_dir/src/pmdk/pipe.cpp $base_dir/src/pmdk/queue.cpp -pthread -lpmemobj -std=c++17
