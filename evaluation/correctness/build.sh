#!/bin/bash
SCRIPT_DIR=`dirname $(realpath "$0")`

# Save option
opt=$1
if [ "$opt" == "--no-persist" ]; then
    echo "no-persist" > config
elif [ "$opt" == "" ]; then
    echo "persist" > config
else
    echo "Invalid option: $opt"
    exit 0
fi

# Build
cfg=$(cat config)
cargo clean
# export RUSTFLAGS="-Z sanitizer=address"
# export ASAN_OPTIONS="detect_leaks=0"

if [ "$cfg" == "no-persist" ]; then
    cargo build --tests --release --features=no_persist --features=tcrash --target x86_64-unknown-linux-gnu
else
    cargo build --tests --release --features=tcrash --target x86_64-unknown-linux-gnu
fi

rm -f $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-*.d
