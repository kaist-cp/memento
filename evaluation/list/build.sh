#!/bin/bash

## Install dependency

sudo apt install libatomic1
sudo apt install libnuma1
sudo apt install libvmmalloc1
sudo apt install libvmem1
sudo apt install libpmem1
sudo apt install build-essential python3-pip
pip3 install --user pandas matplotlib gitpython

## Compile

# (cd src/tracking; ./figures_compile.sh)
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
cargo build --release

