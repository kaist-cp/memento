ANGLE="15 20 25 30"
LIB="clobber pmdk nolog"

rm yada.csv
for A in $ANGLE; do
    for LB in $LIB; do
        for I in `seq 0 4`; do
            if [  -f "Makefile" ]; then
            {
                make clean
                make yada-$LB
            } 1>build.log 2>&1
            fi
            rm -rf /mnt/pmem0/*
            PMEM_IS_PMEM_FORCE=1 ./yada-$LB -a $A -i inputs/ttimeu10000.2 >&data.log
            TIME=`cat data.log | grep 'Elapsed time' | awk '{ print $4 }'`
            echo "${LB},${A},${I},${TIME}">> yada.csv
        done
    done
done
