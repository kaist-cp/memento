# build with only clobber log pass
mkdir tmp
cp -rf passes/* tmp/

rm passes/ClobberFunc.cpp
rm passes/GlobalVal.cpp

sed -i -e 's:MemoryIdempotenceAnalysis.cpp;NaiveHook.cpp;ClobberFunc.cpp;GlobalVal.cpp;:MemoryIdempotenceAnalysis.cpp;NaiveHook.cpp;:g' passes/CMakeLists.txt

./build_llvm.sh
cp build/lib/RollablePasses.so ClobberPass.so

cp -rf tmp/* passes/
rm -rf tmp/

./build_llvm.sh
