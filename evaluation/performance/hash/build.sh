#!/bin/bash

dir_path=$(dirname $(realpath $0))

sudo modprobe msr # https://github.com/sfu-dis/pibench#intel-pcm

# Compile all
cd $dir_path
(cd ../../; cargo update) # update memento crate
make clean
make -j

# # Recompile Dash and Clevel to evaluate the load factor
# make clean -C hash/Dash
# make clean -C hash/Clevel
# rm bin/clevel.so
# make DA_FLAGS=-DCOUNTING -C hash/Dash -j
# make CL_FLAGS=-DDEBUG_RESIZING -j
