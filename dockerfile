FROM rust:1.67

WORKDIR /usr/src/memento
COPY . .

RUN cargo build --release
