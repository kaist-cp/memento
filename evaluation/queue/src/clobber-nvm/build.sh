set -e

# build LLVM with Clobber-NVM passes
echo "Building LLVM, check progress at build.log"
./build_llvm.sh
echo "Done."

# # build mnemosyne and its vacation and memcached
# echo "Building Mnemosyne"
# ./build_mnemosyne.sh
# echo "Done."

# build pmdk and atlas
echo "Building PMDK"
./pmdk.sh
echo "Done."

# echo "Building Atlas"
# ./atlas.sh
# echo "Done."

# build the pass that only includes clobber log
echo "Building Clobber log compiler pass"
./build_clobberpass.sh
echo "Done."

# build the tas lock lib
echo "Building spinlock library"
./build_taslock.sh
echo "Done."

# build runtime
cd apps
echo "Building runtime"
./build_runtime.sh
cd ..
echo "Done."
