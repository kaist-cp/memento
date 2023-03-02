# Detecability Evaluation (§6.1)

We evaluate the detectable recoverability of data structures based on Memento by killing an arbitrary thread and checking if the thread recovers correctly.

## Usage

You can test each data structure with the following command:

```bash
./build.sh # specially built for the thread crash test
./run.sh queue_general
```

where `queue_general` can be replaced with other supported tests (listed below).
This creates test log under `./out`.

## Expected output

TODO: Explain output

## Supported tests

### For primitives

TODO: test method

- `checkpoint`
- `detectable_cas`

### For data structures

TODO: test method

- `queue_general`: ***MSQ-mmt-O0*** in the paper
- `queue_lp`: ***MSQ-mmt-O1*** in the paper
- `queue`: ***MSQ-mmt-O2*** in the paper
- `queue_comb` in the paper
- `treiber_stack`: ***TreiberS-mmt*** in the paper
- `list`: ***List-mmt***
- `clevel`: ***CombQ-mmt*** in the paper
