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

# # Recompile Dash and Clevel to evaluate the load factor
# make clean -C hash/Dash
# make clean -C hash/Clevel
# rm bin/clevel.so
# make DA_FLAGS=-DCOUNTING -C hash/Dash -j
# make CL_FLAGS=-DDEBUG_RESIZING -j
