#!/bin/sh

set -e


DIR_BASE=$(dirname $(realpath $0))
DIR_MMT=$DIR_BASE/../../../
BUILD=$DIR_BASE/build

# Build libmemento.a
cd $DIR_BASE
./build_memento.sh

# Build executable
LLVMDIR=/dev/shm/jaaru/llvm-project # LLVM 14.0.4
LLVMPASS=${LLVMDIR}/build/lib/libPMCPass.so
CC=${LLVMDIR}/build/bin/clang
BUILTIN=/home/ubuntu/.rustup/toolchains/nightly-2022-05-26-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib/libcompiler_builtins-16d69221f10b0282.rlib
PMCHECK=/home/ubuntu/seungmin.jeon/pldi2023-rebuttal/psan-myself/pmcheck
MEMENTO=$BUILD/libmemento.a
RALLOC=$DIR_MMT/ext/ralloc/test/libralloc.a
INCPMCHK="-L${PMCHECK}/bin/ -lpmcheck"
INCLUDEEE=" "
DEPS=$DIR_MMT/target/release/deps
for file in $DEPS/*.rlib
do
    INCLUDEE="$INCLUDEE $file "
done

PARAMS="psan.cpp -o psan -Wall -O2 -g -rdynamic"
$CC $INCPMCHK $PARAMS -Wl,-whole-archive -Wl,-no-whole-archive\
    $MEMENTO\
    $RALLOC\
    $INCLUDEE $INCLUDEE\
    $BUILTIN\
    -lpthread -lpmcheck \
    -lstdc++ -lm -lgcc_s -lgcc -lc -lgcc_s -lgcc
