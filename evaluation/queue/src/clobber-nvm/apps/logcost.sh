#!/bin/bash
BENCHMARKS="bptree skiplist hashmap rbtree"
#BENCHMARKS="hashmap"
#THREADS="1 2 4 8 16"
THREADS="1"
#VALUE_SIZES="128 256 512 1024"
VALUE_SIZES="256"
#NVM_LIBS="nolog"
NVM_LIBS="nolog undo vlog warlog clobber"
MOUNT_POINT=/mnt/pmem0
TRACE_PATH=$1
TIMEOUT=10m

if [ -z "$TRACE_PATH" ]; then
    >&2 echo 'Trace path is empty!'
    exit 1
fi

function run() {
    MODE=$1
    TAG=$2
    WL=$3
    for BENCH in $BENCHMARKS; do
        cd $BENCH
        make clean 1>/dev/null && make benchmark${MODE} 1>/dev/null
        if [ $? -ne 0 ]; then
            >&2 echo "Unable to build: $BENCH/benchmark${MODE}"
            cd ..
            return 1
        fi

        for T in $THREADS; do
            for SZ in $VALUE_SIZES; do
                for I in `seq 0 4`; do
                    sleep 3s
                    rm -rf ${MOUNT_POINT}/*
                    export PMEM_IS_PMEM_FORCE=1
                    timeout -s 9 $TIMEOUT ./benchmark${MODE} \
                        -t $T -d $SZ -w $WL -f $TRACE_PATH -r 1>/tmp/output
                    if [ $? -ne 0 ]; then
                        >&2 echo "Unable to run: $BENCH/benchmark${MODE}"
                        continue
                    fi
                    LOAD=`cat /tmp/output | grep 'Load throughput' | awk '{ print $3 }'`
                	if [ $TAG == 'warlog' ]; then
            	        echo "clobberlog,${BENCH},${T},${I},${SZ},${LOAD}"
        	        else
    	                echo "${TAG},${BENCH},${T},${I},${SZ},${LOAD}"
	                fi

                done
            done
        done
        cd ..
    done
}

{
for WORKLOAD in a; do

    for LIB in $NVM_LIBS; do
        MODE="-${LIB}"

        run $MODE "${LIB}" $WORKLOAD

    done
done
} 1>logcost.csv


exit 0
