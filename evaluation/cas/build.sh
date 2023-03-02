#!/bin/bash

set -e

# Build
(cd ..; cargo update) # update memento crate
cargo update # update evaluation crate
cargo build --release
