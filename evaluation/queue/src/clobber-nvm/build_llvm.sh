#!/bin/bash
LLVM_VER='7.0.0'

# Get LLVM source
if [ ! -d "llvm" ]; then
{
    wget "http://releases.llvm.org/${LLVM_VER}/llvm-${LLVM_VER}.src.tar.xz"
    if [[ $? -ne 0 ]]; then
        echo 'Unable to download LLVM!'
        exit 1
    fi
    tar xf llvm-${LLVM_VER}.src.tar.xz
    if [[ $? -ne 0 ]]; then
        echo 'Unable to extract LLVM archive!'
        exit 1
    fi
    rm -f llvm-${LLVM_VER}.src.tar.xz
    mv llvm-${LLVM_VER}.src llvm
} 1>build.log 2>&1
fi

# Get Clang source
cd llvm/tools
if [ ! -d "clang" ]; then
{
    wget "http://releases.llvm.org/${LLVM_VER}/cfe-${LLVM_VER}.src.tar.xz"
    if [[ $? -ne 0 ]]; then
        echo 'Unable to download Clang!'
        exit 1
    fi
    tar xf cfe-${LLVM_VER}.src.tar.xz
    if [[ $? -ne 0 ]]; then
        echo 'Unable to extract Clang archive!'
        exit 1
    fi
    rm -f cfe-${LLVM_VER}.src.tar.xz
    mv cfe-${LLVM_VER}.src clang
} 1>../../build.log 2>&1
fi
cd ../..

cd llvm/projects
if [ ! -d "compiler-rt" ]; then
{
    wget "http://releases.llvm.org/${LLVM_VER}/compiler-rt-${LLVM_VER}.src.tar.xz"
    if [[ $? -ne 0 ]]; then
        echo 'Unable to download compiler-rt!'
        exit 1
    fi
    tar xf compiler-rt-${LLVM_VER}.src.tar.xz
    if [[ $? -ne 0 ]]; then
        echo 'Unable to extract compiler-rt!'
        exit 1
    fi
    rm -f compiler-rt-${LLVM_VER}.src.tar.xz
    mv compiler-rt-${LLVM_VER}.src compiler-rt
} 1>../../build.log 2>&1
fi
cd ../..

# Get LLD source
cd llvm/tools
if [ ! -d "lld" ]; then
{
    wget "http://releases.llvm.org/${LLVM_VER}/lld-${LLVM_VER}.src.tar.xz"
    if [[ $? -ne 0 ]]; then
        echo 'Unable to download LLD!'
        exit 1
    fi
    tar xf lld-${LLVM_VER}.src.tar.xz
    if [[ $? -ne 0 ]]; then
        echo 'Unable to expand LLD archive!'
        exit 1
    fi
    rm -f lld-${LLVM_VER}.src.tar.xz
    mv lld-${LLVM_VER}.src lld
} 1>../../build.log 2>&1
fi
cd ../..

if [ ! -d "build" ]; then
{
    if [ ! -d 'llvm/lib/Transforms/Passes' ]; then
        ln -s `readlink -f passes` llvm/lib/Transforms/Passes
        echo "add_subdirectory(Passes)" >> llvm/lib/Transforms/CMakeLists.txt
    fi
    mkdir build && cd build
    #cmake -DCMAKE_BUILD_TYPE=DEBUG -G "Unix Makefiles" ../llvm
    cmake -DCMAKE_BUILD_TYPE=RELEASE -G "Unix Makefiles" ../llvm
    if [[ $? -ne 0 ]]; then
        echo 'Unable to create make files!'
        exit 1
    fi
    make -j16
    if [[ $? -ne 0 ]]; then
        echo 'Unable to build LLVM and Clang!'
        exit 1
    fi
    
} 1>build.log 2>&1
else
    cd build
    make -j
    if [[ $? -ne 0 ]]; then
        exit 1
    fi
fi
cd ..
