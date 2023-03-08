include ../common/Makefile_common

CLEVEL_RUST=./clevel_rust
CFLAGS += -L$(CLEVEL_RUST)/target/release -lclevel_rust
CSHARED = -Wl,-whole-archive $(CLEVEL_RUST)/target/release/libclevel_rust.a -Wl,-no-whole-archive

all:clevel_rust.so

clevel_rust.so:clevel_rust.o do_build
	$(CXX) $(CFLAGS) -fPIC -shared -o $@ clevel_rust.o $(CSHARED)

clevel_rust.o:clevel_rust.cpp
	$(CXX) -fPIC -o clevel_rust.o -c clevel_rust.cpp $(CFLAGS)

# pmem
do_build:
	bash -c "cd $(CLEVEL_RUST); cargo update; cargo build --release;"

# dram
do_build_dram:
	bash -c "cd $(CLEVEL_RUST); cargo update; cargo build --release --features no_persist;"

clean:
	rm -f *.o *.so lsb msb *.out
	bash -c "cd $(CLEVEL_RUST); cargo clean"
