#!/bin/bash
PMEM_DEV=/dev/pmem1
if [ -f 'pmem.dev' ]; then
    PMEM_DEV=$(cat pmem.dev)
fi

{
#    umount -f /mnt/pmem0disk
	umount -f /mnt/pmem1
    mkdir -p /mnt/pmem0
    mkfs.ext4 -F $PMEM_DEV
    mount -t ext4 -o dax $PMEM_DEV /mnt/pmem0
    chmod -R 777 /mnt/pmem0
}

