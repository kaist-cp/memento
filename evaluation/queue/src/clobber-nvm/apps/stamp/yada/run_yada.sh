rm -rf /mnt/pmem0/*
if [  -f "Makefile" ]; then
{
    make clean
    make yada-clobber
    make yada-pmdk
    make yada-nolog
} 1>build.log 2>&1
fi

ANGLE="15 20 25 30"

for A in $ANGLE; do
        echo 'Angle Constraint' $A
        PMEM_IS_PMEM_FORCE=1 ./yada-clobber -a $A -i inputs/ttimeu10000.2
        rm -rf /mnt/pmem0/*
        PMEM_IS_PMEM_FORCE=1 ./yada-pmdk -a $A -i inputs/ttimeu10000.2
        rm -rf /mnt/pmem0/*
        PMEM_IS_PMEM_FORCE=1 ./yada-nolog -a $A -i inputs/ttimeu10000.2
        rm -rf /mnt/pmem0/*
done

