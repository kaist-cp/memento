
TODO: 
- README, script 정리
- ext/pmdk-rs 추가?: pmemobj_direct가 rust crate pmemobj_sys의 API로 노출 돼야함.

# Build PMCPass (jaaru-llvm-pass)

```
git clone https://github.com/llvm/llvm-project.git
cd llvm-project
git checkout 29f1039a7285a5c3a9c353d054140bf2556d4c4d <!-- (HEAD, tag: llvmorg-14.0.4) !-->
cp ../../ext/jaaru-llvm-pass llvm-project/llvm/lib/Transforms/PMCPass

echo "add_subdirectory(PMCPass)" >> llvm-project/llvm/lib/Transforms/CMakeLists.txt

cd llvm-project
mkdir build
cd build
cmake -DLLVM_ENABLE_PROJECTS=clang -G "Unix Makefiles" ../llvm
make -j
```

# Build PMCheck

```
git clone https://github.com/uci-plrg/jaaru.git
mv jaaru pmcheck
cd pmcheck/
git checkout psan
make -j
```

<!-- # Setting LLVMDIR and JAARUDIR in wrapper scripts
sed -i 's/LLVMDIR=.*/LLVMDIR=~\/llvm-project\//g' Test/gcc
sed -i 's/JAARUDIR=.*/JAARUDIR=~\/pmcheck\/bin\//g' Test/gcc
sed -i 's/LLVMDIR=.*/LLVMDIR=~\/llvm-project\//g' Test/g++
sed -i 's/JAARUDIR=.*/JAARUDIR=~\/pmcheck\/bin\//g' Test/g++
# Building test cases
make test -->

# Build libmemento.a and executable file

```
./build.sh
```

# Run

```
./run.sh <target> <mode>
```

where:
- target: `checkpoint`, `detectable_cas`, `queue_O0`, TODO
- mode: `yashme`, `psan`

