#!/bin/bash

sudo modprobe msr # https://github.com/sfu-dis/pibench#intel-pcm 

# Install dependency
sudo apt install libpmemobj-dev -y
sudo apt install libvmem-dev -y
sudo apt install libgflags-dev -y
sudo apt install build-essential python3-pip -y
pip3 install --user pandas matplotlib

# Compile all
(cd ../../; cargo update) # update memento crate
make clean
make -j

# # Recompile Dash with `DA_FLAGS=-DCOUNTING` to evaluate the load factor 
# make clean -C hash/Dash
# make DA_FLAGS=-DCOUNTING -C hash/Dash -j
# make -j
