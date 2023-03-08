# Clobber-NVM
Clobber-NVM (ASPLOS'21) is a joint compiler/runtime library.

Here's its code organization:

```
apps/------------------Include 7 benchmarks, and Clobber-NVM runtimes.
mnemosyne-gcc/---------Mnemosyne directory, contains the 7 benchmarks adapted on Mnemosyne.
passes/----------------The Clobber-NVM compiler passes.
taslock/---------------The spinlock library.
traces/----------------The YCSB traces.
build_*.sh-------------The scripts that builds necessary components for the Clobber-NVM artifact.
atlas.sh---------------The script that builds Atlas library.
pmdk.sh----------------The script that builds and installs PMDK, Clobber-NVM runtime relies on PMDK.
run_mnemosyne.sh-------The script that builds and runs Mnemosyne-based Memcached and Vacation.
run_all.sh-------------The script that runs all benchmarks.
*clang-----------------A list of clangs that the benchmarks use
```

First, make sure to have a NVMM file-system mounted at ```/mnt/pmem0```.
Check ``apps/ext4.sh`` for instructions.

We evaluated Clobber-NVM on Ubuntu 18.04, with GNU 7.3.1, and LLVM 7.0.0.

To build and run all benchmarks, simply use: ``./build_and_run.sh``. You will need to run it with ``root`` permission. It takes hours to finish, depend on the hardware.

To run each test, follow the instructions below:
1. Build necessary components:
```
./build.sh
```

2. Run data structures with four libraries: Clobber-NVM, PMDK, Mnemosyne and Atlas
```
cd apps
/* produce results in apps/data.csv */
./nvbench.sh ../../traces
```

3. Run log stats and log cost test for Clobber-NVM.
```
cd apps
/* produce results in apps/logcost.csv and apps/logstats.csv */
./logstats.sh ../../traces
./logcost.sh ../../traces
```

4. Run memcached and stamp with clobber and pmdk
```
cd apps
/* produce results in apps/memcached.csv and apps/vacation.csv and yada.csv */
./buildandrun_memcached.sh
./run_stamp.sh
```

5. Run mnemosyne memcached and vacation
```
/* produce results in vacation_mnemosyne.csv and memcached_mnemosyne.csv */
./run_mnemosyne.sh
```


Useful links:

PMDK: https://pmem.io/pmdk/

Mnemosyne: https://github.com/snalli/mnemosyne-gcc

Atlas: https://github.com/HewlettPackard/Atlas

STAMP: https://github.com/kozyraki/stamp

Memcached: http://memcached.org/
