include ../common/Makefile_common

CLEVEL_RUST=./clevel_rust
CFLAGS += -L$(CLEVEL_RUST)/target/release -lpmemobj -lclevel_rust -lpmemobj
CSHARED = -Wl,-whole-archive $(CLEVEL_RUST)/target/release/libclevel_rust.a -Wl,-no-whole-archive

BUILD=do_build
ifeq ($(MAKECMDGOALS),)
    # do nothing
else
    BUILD = $(word 2, $(MAKECMDGOALS))
endif

all:clevel_rust.so

clevel_rust.so:clevel_rust.o $(BUILD)
	$(CXX) $(CFLAGS) -fPIC -shared -o $@ clevel_rust.o $(CSHARED) -lpmemobj

clevel_rust.o:clevel_rust.cpp
	$(CXX) -fPIC -o clevel_rust.o -c clevel_rust.cpp $(CFLAGS)

# pmem
do_build:
	bash -c "cd $(CLEVEL_RUST); cargo update; cargo build --release;"

# pmdk
pmdk:
	bash -c "cd $(CLEVEL_RUST); cargo update; cargo build --release --features pmdk;"

# dram
do_build_dram:
	bash -c "cd $(CLEVEL_RUST); cargo update; cargo build --release --features no_persist;"

clean:
	rm -f *.o *.so lsb msb *.out
	bash -c "cd $(CLEVEL_RUST); cargo clean"
