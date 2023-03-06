#!/bin/bash

# TODO: Generalize path variables

set -e

DIR_BASE=$(dirname $(realpath $0))/..
DIR_MMT=$DIR_BASE/../../../
BUILD=$DIR_BASE/build

# Build executable
LLVMDIR=$BUILD/llvm-project # LLVM 14.0.4
LLVMPASS=$LLVMDIR/build/lib/libPMCPass.so
CC=$LLVMDIR/build/bin/clang
BUILTIN=/home/ubuntu/.rustup/toolchains/nightly-2022-05-26-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib/libcompiler_builtins-16d69221f10b0282.rlib
PMCHECK=$BUILD/pmcheck
MEMENTO=$BUILD/libmemento.a
RALLOC=$DIR_MMT/ext/ralloc/test/libralloc.a
INCPMCHK="-L${PMCHECK}/bin/ -lpmcheck"
INCLUDEEE=" "
DEPS=$DIR_MMT/target/release/deps
for file in $DEPS/*.rlib
do
    INCLUDEE="$INCLUDEE $file "
done

PARAMS="$DIR_BASE/psan.cpp -o $BUILD/psan -Wall -O2 -g -rdynamic"
# PMEMOBJ=/home/ubuntu/seungmin.jeon/pldi2023-rebuttal/psan-myself/memento/target/release/build/pmemobj-sys-a602c8d28ed82576/out/build/src/nondebug/libpmemobj.so

$CC $INCPMCHK $PARAMS -Wl,-whole-archive -Wl,-no-whole-archive\
    $MEMENTO\
    $RALLOC\
    $INCLUDEE $INCLUDEE\
    $BUILTIN\
    -lpthread\
    -lstdc++ -lm -lgcc_s -lgcc -lc -lgcc_s -lgcc\
    $INCLUDEE $INCLUDEE\
    -lpmemobj\
