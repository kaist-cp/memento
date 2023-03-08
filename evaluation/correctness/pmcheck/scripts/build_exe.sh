#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))/..
DIR_MMT=$DIR_BASE/../../../
BUILD=$DIR_BASE/.build

# Build executable
LLVMDIR=$BUILD/llvm-project # LLVM 14.0.4
LLVMPASS=$LLVMDIR/build/lib/libPMCPass.so
CC=$LLVMDIR/build/bin/clang
RUSTUP_PATH=$(rustc --print sysroot)
BUILTIN_NAME=$(ls ${RUSTUP_PATH}/lib/rustlib/x86_64-unknown-linux-gnu/lib/ | grep "libcompiler_builtins-")
BUILTIN_PATH="${RUSTUP_PATH}/lib/rustlib/x86_64-unknown-linux-gnu/lib/${BUILTIN_NAME}"
MEMENTO=$BUILD/libmemento.a
RALLOC=$DIR_MMT/ext/ralloc/test/libralloc.a
INCLUDEEE=" "
DEPS=$DIR_MMT/target/release/deps
for file in $DEPS/*.rlib
do
    INCLUDEE="$INCLUDEE $file "
done

function build() {
    tool=$1
    echo "Build test_mmt_$tool"
    INCPMCHK="-L$BUILD/pmcheck_$tool/bin/ -lpmcheck"
    PARAMS="$DIR_BASE/test_mmt.cpp -o $BUILD/test_mmt_$tool -Wall -O2 -g -rdynamic"

    $CC $INCPMCHK $PARAMS -Wl,-whole-archive -Wl,-no-whole-archive\
        $MEMENTO\
        $RALLOC\
        $INCLUDEE $INCLUDEE\
        $BUILTIN_PATH\
        -lpthread\
        -lstdc++ -lm -lgcc_s -lgcc -lc -lgcc_s -lgcc\
        $INCLUDEE $INCLUDEE\
        -lpmemobj
}

build psan
build yashme
