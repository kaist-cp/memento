#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))/../
BUILD=$DIR_BASE/build
mkdir -p $BUILD

# ./scripts/build_pmcpass.sh
./scripts/build_memento.sh
./scripts/build_pmcheck.sh
./scripts/build_exe.sh
