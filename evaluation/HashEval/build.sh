#!/bin/bash

# Install dependency
sudo modprobe msr # TODO: vldb repo에서 필요하다는데 왜 필요하지? 
sudo apt install libpmemobj-dev -y
sudo apt install libvmem-dev -y
sudo apt install libgflags-dev -y

# Compile all
make clean
make -j

# # Recompile Dash with `DA_FLAGS=-DCOUNTING` to evaluate the load factor 
# make clean -C hash/Dash
# make DA_FLAGS=-DCOUNTING -C hash/Dash -j
# make -j
