#!/bin/bash

dir_path=$(dirname $(realpath $0))

## Compile
cd $dir_path
# (cd src/tracking; ./figures_compile.sh)
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
cargo build --release
