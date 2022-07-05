# Correctness Evaluation

We evaluate the correctness of data structures based on memento. We assess correctness by randomly killing processes or threads and checking their recovery is correct.

## Thread-crash and Recovery

```bash
./tcrash_recovery.sh
```

This creates test log under `./out`.

