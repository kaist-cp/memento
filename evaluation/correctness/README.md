# Detecability Evaluation (§6.1)

We evaluate the detectability in case of thread crashes by randomly crashing an arbitrary thread while running the integration test. To crash a specific thread, we use the tgkill system call to send the SIGUSR1 signal to the thread and let its signal handler abort its execution.

## Usage

You can test each data structure with the following command:

```bash
./build.sh # specially built for the thread crash test
./run.sh [tested DS]
```

where `tested DS` should be replaced with one of supported tests (listed below).
For example, the following command is to infinitely check that the test of ***MSQ-mmt-O0*** in the paper always pass in case of an unexpected thread crash:

```bash
./run.sh queue_general
```

Then the output is printed out like below:

```
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 1 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
[Test 1] success
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 2 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
[Test 2] success
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 3 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
[Test 3] success
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 4 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
^C
```

It also creates a short progress log and a full test log under `./out`.

If a bug exists (just for an example), the output is like below:

```
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 1 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
./run.sh: line 51: 855011 Aborted                 RUST_BACKTRACE=1 RUST_MIN_STACK=2000000000 numactl --cpunodebind=0 --membind=0 timeout $TIMEOUT $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-* $target::test --nocapture &>> $log_tmp
fails with exit code 134
[Test 1] fails with exit code 134
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 2 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
^C
```

It then generates a bug directory consisting of a text file containg specific error log (`info.txt`) and a PM pool files (`queue_general.pool_*`) of the buggy execution so that we can debug the data structure using it.

For each primitive and DS, we observe *no* test failures for 100K runs with thread crashes.

## Supported tests

### For primitives

- `checkpoint`
- `detectable_cas`

### For data structures

- `queue_general`: ***MSQ-mmt-O0*** in the paper
- `queue_lp`: ***MSQ-mmt-O1*** in the paper
- `queue`: ***MSQ-mmt-O2*** in the paper
- `queue_comb` ***CombQ-mmt***in the paper
- `treiber_stack`: ***TreiberS-mmt*** in the paper
- `list`: ***List-mmt***
- `clevel`: ***Clevel-mmt*** in the paper
