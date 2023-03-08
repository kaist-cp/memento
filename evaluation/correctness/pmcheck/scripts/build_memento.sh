#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))/../
DIR_MMT=$DIR_BASE/../../..

TARGET="memento"
BUILD=$DIR_BASE/.build
mkdir -p $BUILD

# 1. Rust src -> Rust IR
echo "emit ir"
cargo clean
cargo rustc --lib --release --features=pmcheck -- --emit=llvm-ir
cp $DIR_MMT/target/release/deps/${TARGET}-*.ll $BUILD/$TARGET.ll
echo "good"

# 2. IR Instrumenting using PMCPass
LLVMDIR=$BUILD/llvm-project # LLVM 14.0.4
LLVMPASS=${LLVMDIR}/build/lib/libPMCPass.so
CC=${LLVMDIR}/build/bin/clang++
OPT=${LLVMDIR}/build/bin/opt
echo "instrument llvm"
$OPT -load ${LLVMPASS} -PMCPass -enable-new-pm=0 $BUILD/$TARGET.ll -o $BUILD/${TARGET}_instrumented.ll
echo "good"

# 3. Compile IR into library
RUSTUP_PATH=$(rustc --print sysroot)
STD_NAME=$(ls $RUSTUP_PATH/lib/ | grep "libstd-")
SO_PATH="${RUSTUP_PATH}/lib/${STD_NAME}"

INCLUDEEE=" "
DEPS=$DIR_MMT/target/release/deps
cd $DIR_MMT/target/release/deps
for file in *.rlib
do
    echo "$file"
    INCLUDEE="$INCLUDEE -L $DEPS -l:$file "
done

BUILTIN_NAME=$(ls ${RUSTUP_PATH}/lib/rustlib/x86_64-unknown-linux-gnu/lib/ | grep "libcompiler_builtins-")
BUILTIN_PATH="${RUSTUP_PATH}/lib/rustlib/x86_64-unknown-linux-gnu/lib/${BUILTIN_NAME}"
cd $DIR_BASE

echo "compile libmemento.a"
$CC -c \
    $BUILD/${TARGET}_instrumented.ll -o $BUILD/$TARGET.o \

ar rcs $BUILD/libmemento.a $BUILD/$TARGET.o \
    $SO_PATH \
    $BUILTIN_PATH

# ar rcs $BUILD/libmemento.a $BUILD/$TARGET.o

echo "Building memento complete."
