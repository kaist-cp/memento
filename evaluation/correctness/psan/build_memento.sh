#!/bin/sh

# TODO: Generalize path variables

set -e

DIR_BASE=$(dirname $(realpath $0))
DIR_MMT=$DIR_BASE/../../..

TARGET="memento"
BUILD=$DIR_BASE/build
mkdir -p $BUILD

# 1. Rust src -> Rust IR
echo "emit ir"
cargo clean
# cargo rustc --lib --release -- --emit=llvm-ir
cargo rustc --lib --release --features=pmcheck -- --emit=llvm-ir
cp $DIR_MMT/target/release/deps/${TARGET}-*.ll $BUILD/$TARGET.ll
echo "good"

# 2. IR Instrumenting using PMCPass
LLVMDIR=/dev/shm/jaaru/llvm-project # LLVM 14.0.4
LLVMPASS=${LLVMDIR}/build/lib/libPMCPass.so
CC=${LLVMDIR}/build/bin/clang++
OPT=${LLVMDIR}/build/bin/opt
echo "instrument llvm"
$OPT -load ${LLVMPASS} -PMCPass -enable-new-pm=0 $BUILD/$TARGET.ll -o $BUILD/${TARGET}_instrumented.ll
echo "good"

# 3. Compile IR into library
TOOLCHAIN="nightly-2022-05-26-x86_64-unknown-linux-gnu"
RSTD="std-2ef13b7c460b887d"
INCRSTD="-L /home/ubuntu/.rustup/toolchains/${TOOLCHAIN}/lib -l${RSTD}"
INC_RALLOC="-L ext/ralloc/test -lralloc"

INCLUDEEE=" "
DEPS=$DIR_MMT/target/release/deps
cd $DIR_MMT/target/release/deps
for file in *.rlib
do
    echo "$file"
    INCLUDEE="$INCLUDEE -L $DEPS -l:$file "
done

BUILTIN=libcompiler_builtins-16d69221f10b0282.rlib
BUILTIN_PATH=/home/ubuntu/.rustup/toolchains/nightly-2022-05-26-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib/
cd $DIR_BASE
pwd

echo "compile libmemento.a"
$CC -c \
    -lpmcheck\
    $BUILD/${TARGET}_instrumented.ll -o $BUILD/$TARGET.o \
    $INCRSTD \
    $INCLUDEE $INCLUDEE\
    -L $BUILTIN_PATH -l:$BUILTIN\
    $INC_RALLOC -lstdc++ \
    -lgcc_s -lutil -lrt -lm -ldl -lc \
    -fPIC \
    -lpmcheck

ar rcs $BUILD/libmemento.a $BUILD/$TARGET.o \
    /home/ubuntu/.rustup/toolchains/${TOOLCHAIN}/lib/lib${RSTD}.so \
    $BUILTIN_PATH/$BUILTIN

echo "good"
