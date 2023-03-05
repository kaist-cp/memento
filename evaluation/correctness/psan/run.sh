#!/bin/bash

set -e

RUSTSTD=/home/ubuntu/.rustup/toolchains/nightly-2022-05-26-x86_64-unknown-linux-gnu/lib
PMCHECK=/home/ubuntu/seungmin.jeon/pldi2023-rebuttal/psan-myself/pmcheck/bin/
TARGET=$1
MODE=$2
OPT=""
if [ "${MODE}" == "yashme" ]; then
    # Yashme (https://github.com/uci-plrg/pmrace-vagrant/blob/master/data/pmdk-races.sh)
    # echo 'export PMCheck="-d$3 -y -x25 -r1000"' >> run.sh
    OPT="-y -x25"
elif [ "${MODE}" == "psan" ]; then
    # PSan (https://github.com/uci-plrg/psan-vagrant/blob/master/data/pmdk-bugs.sh)
    # STRATEGY=-o2
    # export PMCheck=\"-d\$3 ${STRATEGY} -r1787250\"" >> run.sh
    OPT="-o2"
else
    echo "invalid mode: $MODE (possible mode: yashme, psan)"
    exit
fi
echo "target: $TARGET, mode: $MODE, option: $OPT\n"

export LD_LIBRARY_PATH=$PMCHECK:$RUSTSTD
export PMCheck="-d/mnt/pmem0/test/$TARGET/$TARGET.pool_valid $OPT"
# rm -rf PMCheckOutput*
rm -rf /mnt/pmem0/*
ulimit -s 82920000
RUST_MIN_STACK=1000000000 ./psan $TARGET

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
