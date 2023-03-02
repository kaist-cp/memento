#!/bin/sh

set -e

RUSTSTD=/home/ubuntu/.rustup/toolchains/nightly-2022-05-26-x86_64-unknown-linux-gnu/lib
# RUSTSTD=/home/ubuntu/.rustup/toolchains/nightly-2020-06-01-x86_64-unknown-linux-gnu/lib
PMCHECK=/home/ubuntu/seungmin.jeon/pldi2023-rebuttal/psan-myself/pmcheck/bin/

# export LD_LIBRARY_PATH=$PMCHECK:$RUSTSTD
export LD_LIBRARY_PATH=$PMCHECK:$RUSTSTD

#  -L /home/ubuntu/.rustup/toolchains/nightly-2020-06-01-x86_64-unknown-linux-gnu/lib

 # -lstd-effebfe9e2ceaa23

# export PMCheck="-v -o"
# export PMCheck="-d/mnt/pmem0/test -v3 -r10000 -s"
# export PMCheck="-d/mnt/pmem0/test -v3 -s -p -o"
# export PMCheck="-d/mnt/pmem0/test -v3 -s -p -o"
# export PMCheck="-d/mnt/pmem0/test -v3 -s -p -o"
# export PMCheck="-v3 -p -o -s -y"
# export PMCheck="-d/mnt/pmem0/test/queue_general/queue_general.pool_valid -p -o3 -y"
# export PMCheck="-d/mnt/pmem0/test/queue_general/queue_general.pool_valid -v3 -y -x1 -p"
export PMCheck="-d/mnt/pmem0/test/queue_general/queue_general.pool_valid -v3 -p -y"

rm -rf PMCheckOutput*
rm -rf /mnt/pmem0/*

ulimit -s 82929000
# ./psan 2>&1 > psan.out
./psan


# 	model_print(
# 		"Copyright (c) 2021 Regents of the University of California. All rights reserved.\n"
# 		"Written by Hamed Gorjiara, Brian Demsky, Peizhao Ou, Brian Norris, and Weiyu Luo\n"
# 		"\n"
# 		"Usage: PMCheck=[MODEL-CHECKER OPTIONS]\n"
# 		"\n"
# 		"MODEL-CHECKER OPTIONS can be any of the model-checker options listed below. Arguments\n"
# 		"provided after the `--' (the PROGRAM ARGS) are passed to the user program.\n"
# 		"\n"
# 		"Model-checker options:\n"
# 		"-h, --help                  Display this help message and exit\n"
# 		"-v[NUM], --verbose[=NUM]    Print verbose execution information. NUM is optional:\n"
# 		"                              0 is quiet; 1 shows valid executions; 2 is noisy;\n"
# 		"                              3 is noisier.\n"
# 		"                              Default: %d\n"
# 		"-p                          PMDebug level\n"
# 		"-t                          Dump Stack at Crash Injection\n"
# 		"-f                          Memory initialization byte\n"
# 		"-r                          model clock for first possible crash\n"
# 		"-n                          No fork\n"
# 		"-s                          Print size of exploration space\n"
# 		"-c                          Number of nested crashes\n"
# 		"                            Default: %u\n"
# 		"-d [file]					 Deleting the persistent file after each execution.\n"
# 		"-e							 Enable manual crash point.\n"
# 		"-x							 Enable random execution (default execution number = 30)\n"
# 		"-o							 Enable Verifier analysis (Default: Naive mode=1)\n"
# 		"								1: Naive: Report bug and continue\n"
# 		"								2: Exit: exit on first error\n"
# 		"								3: Safe: Forcing to explore robustness violation-free writes\n"
# 		"-a							 Initializing random seed (default seed = 423121)\n"
# 		"-o							 Enable Verifier analysis\n"
# 		"-b							 Threashold for randomly evict instructions from store buffer (Default = 15)\n"
# 		"-y							 Enable Persistency race analysis\n",
# 		params->verbose, params->numcrashes);
# 	exit(EXIT_SUCCESS);
# }
