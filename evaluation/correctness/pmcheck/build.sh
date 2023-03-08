#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))

cd $DIR_BASE
mkdir -p .build
./scripts/build_memento.sh
./scripts/build_pmcheck.sh
./scripts/build_exe.sh
