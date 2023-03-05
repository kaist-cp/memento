#!/bin/bash 

# Build PMCPass (jaaru-llvm-pass)

set -e

DIR_BASE=$(dirname $(realpath $0))/../
DIR_MMT=$DIR_BASE/../../..
BUILD=$DIR_BASE/build
mkdir -p $BUILD
cd $BUILD
pwd

# git clone https://github.com/llvm/llvm-project.git
cd llvm-project
git checkout 29f1039a7285a5c3a9c353d054140bf2556d4c4d # tag: llvmorg-14.0.4
cp -r $DIR_MMT/ext/jaaru-llvm-pass llvm/lib/Transforms/PMCPass

echo "add_subdirectory(PMCPass)" >> llvm/lib/Transforms/CMakeLists.txt

mkdir -p build
cd build
cmake -DLLVM_ENABLE_PROJECTS=clang -G "Unix Makefiles" ../llvm
# cmake -DLLVM_ENABLE_PROJECTS=clang -DCMAKE_BUILD_TYPE=Release -G "Unix Makefiles" # doesn't work
make -j