#!/bin/sh
PMDK_VER='1.8'
{
rm -rf pmdk
wget https://github.com/pmem/pmdk/archive/${PMDK_VER}.tar.gz
if [ $? -ne 0 ]; then
    echo 'Unable to download PMDK'
    exit 1
fi
tar -xzvf ${PMDK_VER}.tar.gz
if [ $? -ne 0 ]; then
    echo 'Unable to extract the archive'
    exit 1
fi
rm -f ${PMDK_VER}.tar.gz
mv pmdk-${PMDK_VER} pmdk
cd pmdk

make -j15
if [ $? -ne 0 ]; then
    echo 'Unable to make PMDK'
    exit 1
fi
cd ..
} 1>pmdk.log 2>&1

cd pmdk
make install -j15
if [ $? -ne 0 ]; then
    echo 'Unable to install PMDK!'
    exit 1
fi
cd ..

exit 0
