# Correctness Evaluation

We evaluate the correctness of data structures based on memento. We assess correctness by randomly killing thread and checking their recovery is correct.

## Thread-crash and Recovery

```bash
./build.sh
./tcrash_recovery.sh $target
```

where `target`: queue_general, queue_lp, queue, queue_comb, elim_stack, list, clevel

This creates test log under `./out`.

