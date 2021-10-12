#!/bin/bash

# TODO? install libndctl-dev?
dir_path=$(dirname $(realpath $0))

# Install dependency for plot
sudo apt install build-essential python3-pip
pip3 install --user pandas matplotlib

# Build Rust implementation
cargo build --release --examples;

# Build C++ implementation
$dir_path/examples_cpp/build.sh
