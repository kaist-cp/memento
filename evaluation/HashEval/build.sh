#!/bin/bash

# sudo modprobe msr
sudo apt install libpmemobj-dev -y
sudo apt install libvmem-dev -y
sudo apt install libgflags-dev -y

# Compile all
make clean
make -j

# # For Dash, recompile with `DA_FLAGS=-DCOUNTING`
# make clean -C hash/Dash
# make DA_FLAGS=-DCOUNTING -C hash/Dash -j
# make -j
