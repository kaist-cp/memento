#!/bin/bash

# Create bin folder
mkdir -p bin
rm -rf bin/*

make -f Makefile_Linked_List ll-recoverable-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"
make -f Makefile_Linked_List ll-recoverable-nopsync-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"
make -f Makefile_Linked_List ll-recoverable-nopwbs-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"
make -f Makefile_Linked_List ll-recoverable-nolowpwbs-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"
make -f Makefile_Linked_List ll-recoverable-nolownomedpwbs-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"
make -f Makefile_Linked_List ll-recoverable-lowpwbs-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"
make -f Makefile_Linked_List ll-recoverable-medpwbs-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"
make -f Makefile_Linked_List ll-recoverable-highpwbs-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"
make -f Makefile_Linked_List capsules-read ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.15 -DDELETES_PERCENTAGE=0.15"

make -f Makefile_Linked_List ll-recoverable-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
make -f Makefile_Linked_List ll-recoverable-nopsync-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
make -f Makefile_Linked_List ll-recoverable-nopwbs-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
make -f Makefile_Linked_List ll-recoverable-nolowpwbs-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
make -f Makefile_Linked_List ll-recoverable-nolownomedpwbs-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
make -f Makefile_Linked_List ll-recoverable-lowpwbs-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
make -f Makefile_Linked_List ll-recoverable-medpwbs-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
make -f Makefile_Linked_List ll-recoverable-highpwbs-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
make -f Makefile_Linked_List capsules-update ARGS="-DKEY_RANGE=500 -DINSERTS_PERCENTAGE=0.35 -DDELETES_PERCENTAGE=0.35"
