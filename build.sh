#!/bin/bash

# Install dependency for plot
sudo apt install build-essential python3-pip
pip3 install --user pandas matplotlib

# Build Rust implementation
cargo build --release --examples;

# Build C++ implementation
# TODO: pmdk