#!/bin/bash
apt-get install -y wget
apt-get install -y xz-utils
apt-get install -y cmake
apt-get install -y build-essential
apt-get install -y python
wget https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip install pandas
apt-get install -y autoconf autogen
apt-get install -y numactl
apt-get install -y libevent-dev
apt-get install -y libjemalloc-dev
apt-get install -y llvm clang-3.9 # Atlas
apt-get install -y ruby # Atlas
apt-get install -y libboost-graph-dev # Atlas

apt-get install -y libndctl-dev
apt-get install -y pkg-config

apt-get install -y libdaxctl-dev
apt-get install -y scons
apt-get install -y libconfig-dev
apt-get install -y libelf-dev

apt-get install -y pandoc

exit 0

