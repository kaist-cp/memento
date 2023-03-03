FROM rust:1.67

WORKDIR /usr/src/memento
COPY . .

RUN apt-get update && \
    apt-get install -y \
    build-essential python3-pip numactl \
    libpmemobj-dev libvmem-dev libgflags-dev \
    libpmemobj1 libpmemobj-cpp-dev \
    libatomic1 libnuma1 libvmmalloc1 libvmem1 libpmem1 \
    kmod sudo && \
    pip3 install --user pandas matplotlib gitpython && \
    sudo $base_dir/src/clobber-nvm/deps.sh && \
    cargo build --release
