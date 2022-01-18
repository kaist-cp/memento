include ../common/Makefile_common

SOFT_rust=./SOFT_rust
CFLAGS += -L$(SOFT_rust)/target/release -lSOFT_rust
CSHARED = -Wl,-whole-archive $(SOFT_rust)/target/release/libSOFT_rust.a -Wl,-no-whole-archive

all:SOFT_rust.so

# NOTE: default는 pmem 버전 컴파일. dram에서 실험해보고 싶다면 `do_build`->`do_build_dram`
SOFT_rust.so:SOFT_rust.o do_build
	$(CXX) $(CFLAGS) -fPIC -shared -o $@ SOFT_rust.o $(CSHARED)

SOFT_rust.o:SOFT_rust.cpp
	$(CXX) -fPIC -o SOFT_rust.o -c SOFT_rust.cpp $(CFLAGS)

# pmem 버전 컴파일
do_build:
	bash -c "cd $(SOFT_rust); cargo build --release"

# dram 버전 컴파일
do_build_dram:
	bash -c "cd $(SOFT_rust); cargo build --release --features no_persist;"

clean:
	rm -f *.o *.so lsb msb *.out
	bash -c "cd $(SOFT_rust); cargo clean"
