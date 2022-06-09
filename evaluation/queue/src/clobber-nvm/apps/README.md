### This directory contains the following contents:

```
bptree/------------------The bptree benchmark.
hashmap/-----------------The hashmap benchmark.
rbtree/------------------The rbtree benchmark.
skiplist/----------------The skiplist benchmark.
context/-----------------Contains some runtime files.
runtime/-----------------Contains some runtime files.
memcached/---------------The memcached benchmark.
stamp/-------------------The vacation and yada benchmark.
*.sh---------------------A list of useful scripts.
```
Before run any of following experiments, make sure to have the runtime built. 
You can build the runtime by ``./build_runtime.sh``

1. To build and run the four data structures, use:
```
./nvbench ../../traces
```
It runs four data structures with Clobber-NVM, pmdk, Atlas and Mnemosyne versions.
The results are reported in ``data.csv``.

2. To build and run memcached, use:
```
./buildandrun_memcached.sh
```
The results are reported in ``memcached.csv``.

To build and run a specific version (specific library with specific locking), use:
```
./build_memcached.sh <LOCK> <LIB>
./run_memcache.sh <LOCK> <LIB>
```
The valid options are:
```
mutex pmdk, mutex clobber
wrlock pmdk, wrlock clobber
```
Create a ``run_<WRITE PERCENTAGE>.cnf `` under ``../mnemosyne-gcc/usermode/`` and 
update the ``WORKLOAD`` in ``run_memcache.sh`` to adjust workloads.

3. To build and run Vacation and Yada, use:

```
./run_stamp.sh
```

The results are reported in ``vacation.csv`` and ``yada.csv``.

To build and run a specific version (specific underlying data structure), use:

```
cd stamp/vacation
./run_avltree.sh
./run_rbtree.sh
```
The results of Clobber-NVM and PMDK versions are reported.

