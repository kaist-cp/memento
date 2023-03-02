#!/bin/bash

## Compile

# (cd src/tracking; ./figures_compile.sh)
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
cargo build --release

