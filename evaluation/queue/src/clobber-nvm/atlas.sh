#!/bin/sh
{
rm -rf Atlas
wget https://github.com/HewlettPackard/Atlas/archive/master.tar.gz
if [ $? -ne 0 ]; then
    echo 'Unable to download Atlas'
    exit 1
fi
tar -xzvf master.tar.gz
if [ $? -ne 0 ]; then
    echo 'Unable to extract the archive'
    exit 1
fi
rm -f master.tar.gz
mv Atlas-master Atlas
cd Atlas

# Requires Atlas 3.9
#export PATH="/usr/lib/llvm-3.9/bin/:$PATH"
export PATH="/root/Yi/llvm/build/bin:$PATH"

#build compiler
cd compiler-plugin
if [ ! -d "build-all" ]; then
    ./build_plugin
fi

#set wrapper flags
#export CFLAGS="-Wl,-wrap,pthread_mutex_lock"
#export CFLAGS="$CFLAGS -Wl,-wrap,pthread_mutex_trylock"
#export CFLAGS="$CFLAGS -Wl,-wrap,pthread_mutex_unlock"
#export CFLAGS="$CFLAGS -Wl,-wrap,pthread_create"
#export CFLAGS="$CFLAGS -Wno-error=unused-command-line-argument"

#build runtime
cd ..
cd runtime
sed -i 's:-O3:-O3 -fPIC -D_DISABLE_HELPER:g' CMakeLists.txt
echo 'add_library (atlas-shared SHARED $<TARGET_OBJECTS:Cache_flush> $<TARGET_OBJECTS:Consistency> $<TARGET_OBJECTS:Logger> $<TARGET_OBJECTS:Util> $<TARGET_OBJECTS:Pregion_mgr> $<TARGET_OBJECTS:Pmalloc>) #defaults to static build' >> CMakeLists.txt
sed -i 's:/dev/shm/:/mnt/pmem0/:g' src/util/util.cpp

sed -i 's:kPRegionSize_ = 4:kPRegionSize_ = 18:g' src/internal_includes/pregion_configs.hpp
sed -i 's:kNumArenas_ = 64:kNumArenas_ = 8:g' src/internal_includes/pregion_configs.hpp
#sed -i 's:kHashTableSize = 1 << 10:kHashTableSize = (uint64_t)1 << 16:g' src/internal_includes/log_configs.hpp
sed -i -e '582d;593,594d;596d' src/pregion_mgr/pregion_mgr.cpp

if [ -d "build-all" ];
then
   rm -r build-all
fi

mkdir build-all
cd build-all
cmake ..
make -j8
if [ $? -ne 0 ]; then
    echo 'Unable to make Atlas'
    exit 1
fi

}
exit 0
