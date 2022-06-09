BENCHMARKS="bptree skiplist hashmap rbtree"
VALUE_SIZES="256"
MOUNT_POINT=/mnt/pmem0
TRACE_PATH=$1
TIMEOUT=10m
LOG_TYPE="nolog undo vlog warlog clobber"

{
for BENCH in $BENCHMARKS; do
	cd $BENCH
        for SZ in $VALUE_SIZES; do

        	make clean 1>/dev/null && make benchmark-count 1>/dev/null
        	if [ $? -ne 0 ]; then
            		>&2 echo "Unable to build: $BENCH/benchmark-count"
            	cd ..
            	return 1
        	fi

		sleep 3s
		rm -rf ${MOUNT_POINT}/*
                export PMEM_IS_PMEM_FORCE=1
                timeout -s 9 $TIMEOUT ./benchmark-count \
                    -t 1 -d $SZ -w o -f $TRACE_PATH -r 1>/tmp/output
                if [ $? -ne 0 ]; then
                    >&2 echo "Unable to run: $BENCH/benchmark${MODE}"
                    continue
                fi
                for L in $LOG_TYPE; do
                    LOG=`cat /tmp/output | grep $L | awk '{ print $1 }'`
                    COUNT=`cat /tmp/output | grep $L | awk '{ print $3 }'`
                    SIZE=`cat /tmp/output | grep $L | awk '{ print $5 }'`
				if [ $L == 'warlog' ]; then
    				echo "${BENCH},clobberlog,${SZ},${COUNT}${SIZE}"
				else
    				echo "${BENCH},${L},${SZ},${COUNT}${SIZE}"
				fi

                done
	done
    cd ..
done
} 1>logstats.csv
