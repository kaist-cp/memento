#!/bin/bash
killall memcached


THREADS="1 2 4 8 16"
WORKLOAD="95 75 25 5"

LOCK=$1
LIB=$2

unset LD_PRELOAD
if [ $LOCK == 'mutex' ]; then
	export LIBTXLOCK=tas
	export LD_PRELOAD="$LD_PRELOAD:`realpath ../taslock/tl-pthread-mutex.so`"
fi

{
for T in $THREADS; do
	for W in $WORKLOAD; do
		for I in `seq 0 4`; do
			rm -rf /mnt/pmem0/*
			cp ../mnemosyne-gcc/usermode/run_$W.cnf ../mnemosyne-gcc/usermode/run.cnf
			killall memcached
			PMEM_IS_PMEM_FORCE=1 ./memcached/memcached -u root -p 11215 -l 127.0.0.1 -t $T &
			cd ../mnemosyne-gcc/usermode
        	./run_memslap.sh >&../../apps/data.log
			cd ../../apps
			RATE=`cat data.log | grep 'Net_rate' | awk '{ print $9 }'`
#			echo "${LIB}-${LOCK},${T},${W},${I},${RATE}">> memcached.csv
            if [ $LOCK == 'mutex' ]; then
	            echo "${LIB}-spinlock,${T},${W},${I},${RATE}">> memcached.csv
        	else
                echo "${LIB}-${LOCK},${T},${W},${I},${RATE}">> memcached.csv
            fi
		done
	done
done
} >&output.log
rm output.log
rm data.log

