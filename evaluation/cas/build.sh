#!/bin/bash

set -e
dir_path=$(dirname $(realpath $0))
cd $dir_path

# Build
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
cargo build --release
