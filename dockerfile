FROM rust:1.67

WORKDIR /usr/src/memento
COPY . .

RUN apt update && \
    apt install -y numactl && \
    cargo build --release
