FROM rust:1.67

WORKDIR /usr/src/memento
COPY . .

RUN apt-get update && \
    apt-get install -y \
    build-essential python3-pip numactl libnuma-dev \
    libpmemobj-dev libvmem-dev libgflags-dev \
    libpmemobj1 libpmemobj-cpp-dev \
    libatomic1 libnuma1 libvmmalloc1 libvmem1 libpmem1 \
    clang kmod sudo && \
    pip3 install --user pandas matplotlib gitpython && \
    ulimit -s 8192000 && \
    git submodule update --init --recursive && \
    (cd ext/pmdk-rs; git apply ../pmdk-rs.patch) && \
    evaluation/correctness/tcrash/build.sh && \
    evaluation/performance/cas/build.sh && \
    evaluation/performance/queue/build.sh && \
    evaluation/performance/list/build.sh && \
    evaluation/performance/hash/build.sh && \
    evaluation/correctness/pmcheck/scripts/build_pmcpass.sh && \
    evaluation/correctness/pmcheck/build.sh
