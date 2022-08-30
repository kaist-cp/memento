# Correctness Evaluation

We evaluate the detectable recoverability of data structures based on memento by randomly crashing arbitrary threads and checking if their recovery is correct. You can test each data structure with the following command:

```bash
./build.sh
./run.sh $ds
```

where `ds`: queue_general, queue_lp, queue, queue_comb, elim_stack, list, clevel

This creates test log under `./out`.

Also, you can run the same test on the DRAM environment:

```bash
./build.sh --no-persist
./run.sh $ds
```
