#!/bin/bash

# # export LD_LIBRARY_PATH=/scratch/bdemsky/pmcheck/bin/
# TODO: pmcheck path
export LD_LIBRARY_PATH=/home/ubuntu/seungmin.jeon/pldi2023-rebuttal/psan-myself/pmcheck/bin/
# For Mac OSX
export DYLD_LIBRARY_PATH=/scratch/bdemsky/pmcheck/bin/

ulimit -s 8192000
rm -rf /mnt/pmem0/*
cargo run --release