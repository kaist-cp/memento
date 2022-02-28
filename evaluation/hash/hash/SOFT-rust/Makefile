include ../common/Makefile_common

SOFT_rust=./SOFT_rust
CFLAGS += -L$(SOFT_rust)/target/release -lSOFT_rust
CSHARED = -Wl,-whole-archive $(SOFT_rust)/target/release/libSOFT_rust.a -Wl,-no-whole-archive

all:SOFT_rust.so

SOFT_rust.so:SOFT_rust.o do_build
	$(CXX) $(CFLAGS) -fPIC -shared -o $@ SOFT_rust.o $(CSHARED)

SOFT_rust.o:SOFT_rust.cpp
	$(CXX) -fPIC -o SOFT_rust.o -c SOFT_rust.cpp $(CFLAGS)

# pmem
do_build:
	bash -c "cd $(SOFT_rust); cargo update; cargo build --release"

# dram
do_build_dram:
	bash -c "cd $(SOFT_rust); cargo update; cargo build --release --features no_persist;"

clean:
	rm -f *.o *.so lsb msb *.out
	bash -c "cd $(SOFT_rust); cargo clean"
