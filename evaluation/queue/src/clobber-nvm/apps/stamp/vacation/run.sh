THREADS="1 2 4 8 16"
LIB="clobber pmdk"
TREE="avltree rbtree"
#export LD_PRELOAD="$LD_PRELOAD:`realpath ../../../taslock/tl-pthread-wrlock.so`"

rm vacation.csv
for T in $THREADS; do
    for TR in $TREE; do
        for LB in $LIB; do
			for I in `seq 0 4`; do
            	if [  -f "Makefile" ]; then
            	{
                	if [ $TR == 'avltree' ]; then
                    	sed -i -e 's:CFLAGS += -DMAP_USE_RBTREE:CFLAGS += -DMAP_USE_AVLTREE_LONG -DUSE_DUP_AND_REL:g' Makefile
                	fi
                	if [ $TR == 'rbtree' ]; then
                    	sed -i -e 's:CFLAGS += -DMAP_USE_AVLTREE_LONG -DUSE_DUP_AND_REL:CFLAGS += -DMAP_USE_RBTREE:g' Makefile
                	fi
                	make clean
               		make ../lib/wrap/${LB}_${TR}.o
               		make ../lib/wrap/admin_pop.o
                	make ../lib/wrap/context.o
                	make vacation-${LB}-${TR}
            	} 1>build.log 2>&1
            	fi

                rm -rf /mnt/pmem0/*
                PMEM_IS_PMEM_FORCE=1 ./vacation-$LB-$TR -r100000 -t200000 -n1 -q80 -u99 -c $T >&data.log
                TIME=`cat data.log | grep 'Time' | awk '{ print $3 }'`
		if [ ! -n "$TIME" ]; then
  			echo "Failure happened!"
		else
  			echo "${LB}-${TR},${T},${I},${TIME}">> vacation.csv
		fi
            done
        done
    done
done

rm data.log
rm build.log
