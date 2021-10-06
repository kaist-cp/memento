#!/bin/bash

pmem_path=/mnt/pmem0/
dir_path=$(dirname $(realpath $0))

cd $dir_path; 
cargo build --release --examples;