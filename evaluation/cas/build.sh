#!/bin/bash

set -e

# Install dependency
sudo apt install build-essential python3-pip
pip3 install --user pandas matplotlib gitpython

# Build
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
cargo build --release
