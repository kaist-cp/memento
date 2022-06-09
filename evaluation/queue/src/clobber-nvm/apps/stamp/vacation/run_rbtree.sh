rm -rf /mnt/pmem0/*
if [  -f "Makefile" ]; then
{
    sed -i -e 's:CFLAGS += -DMAP_USE_AVLTREE_LONG -DUSE_DUP_AND_REL:CFLAGS += -DMAP_USE_RBTREE:g' Makefile
    make clean
    make ../lib/wrap/clobber_rbtree.o
    make ../lib/wrap/pmdk_rbtree.o
    make ../lib/wrap/nolog_rbtree.o

    make ../lib/wrap/admin_pop.o
    make ../lib/wrap/context.o

    make vacation-clobber-rbtree
    make vacation-pmdk-rbtree
    make vacation-nolog-rbtree
} 1>build.log 2>&1
fi


THREADS="1 2 4 8 16"
#THREADS="8 16"

for T in $THREADS; do
        echo 'Running with' $T 'threads'
        PMEM_IS_PMEM_FORCE=1 ./vacation-clobber-rbtree -r100000 -t200000 -n1 -q80 -u99 -c $T
        rm -rf /mnt/pmem0/*
        PMEM_IS_PMEM_FORCE=1 ./vacation-pmdk-rbtree -r100000 -t200000 -n1 -q80 -u99 -c $T
        rm -rf /mnt/pmem0/*
done
