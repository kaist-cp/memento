rm -rf /mnt/pmem0/*
if [  -f "Makefile" ]; then
{
    sed -i -e 's:CFLAGS += -DMAP_USE_RBTREE:CFLAGS += -DMAP_USE_AVLTREE_LONG -DUSE_DUP_AND_REL:g' Makefile
    make clean
    make ../lib/wrap/clobber_avltree.o
    make ../lib/wrap/pmdk_avltree.o
    make ../lib/wrap/nolog_avltree.o

    make ../lib/wrap/admin_pop.o
    make ../lib/wrap/context.o

    make vacation-clobber-avltree
    make vacation-pmdk-avltree
    make vacation-nolog-avltree
} 1>build.log 2>&1
fi


THREADS="1 2 4 8 16"

#export LD_PRELOAD="$LD_PRELOAD:`realpath ../../../taslock/tl-pthread-wrlock.so`"

for T in $THREADS; do
        echo 'Running with' $T 'threads'
        PMEM_IS_PMEM_FORCE=1 ./vacation-clobber-avltree -r100000 -t200000 -n1 -q80 -u99 -c $T
        rm -rf /mnt/pmem0/*
        PMEM_IS_PMEM_FORCE=1 ./vacation-pmdk-avltree -r100000 -t200000 -n1 -q80 -u99 -c $T
        rm -rf /mnt/pmem0/*
done
