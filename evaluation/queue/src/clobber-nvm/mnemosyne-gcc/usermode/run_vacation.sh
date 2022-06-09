#!/bin/bash
# sudo to enable trace markers, -e enables tracing
bin=./build/bench/stamp-kozy/vacation/vacation
action=$1

THREADS="1 2 4 8 16"

if [[ $action == '-h' ]]
then
	$bin -h
else
	{
    for T in $THREADS; do
		for I in `seq 0 4`; do
		    rm -rf /mnt/pmem0/*
			$bin -c0 -n1 -r65536 -q100 >&data.log
        	$bin -r100000 -t200000 -n1 -q80 -u99 -c $T >&data.log
			TIME=`cat data.log | grep 'Time' | awk '{ print $3 }'`
			echo "mnemosyne,${T},${I},${TIME}"
		done
    done
	} >&vacation.csv
fi

