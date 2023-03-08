# Persistency Bug Test (Yashme/PSan)

We evaluate the correctness of our primitives and data structures using existing bug finding tools, ***[Yashme](https://plrg.ics.uci.edu/yashme/)*** and ***[PSan](https://plrg.ics.uci.edu/psan/)***. They are finding persistent bugs such as persistency race, missing flushes based on model checking framework ***[Jaaru](https://plrg.ics.uci.edu/jaaru/)***.

## Usage

You can test each data structure with the following command:

```bash
./build.sh # specially built for the persistency bug test
./run.sh [tested DS] [tool] [mode]
```

where 
- `tested DS` should be replaced with one of supported tests (listed below).
- `tool`: `psan` or `yashme`
- `mode`: `model` or `random` (model checking mode or random testing mode)

For example, the following command is to test the ***MSQ-mmt-O0*** using ***PSan*** with model checking mode:

```bash
./run.sh queue_O0 psan model
```

Then the output is printed out like below:

```
Jaaru
Copyright (c) 2021 Regents of the University of California. All rights reserved.
Written by Hamed Gorjiara, Brian Demsky, Peizhao Ou, Brian Norris, and Weiyu Luo

Execution 1 at sequence number 198
nextCrashPoint = 83987	max execution seqeuence number: 88289
nextCrashPoint = 2876	max execution seqeuence number: 4161
Execution 2 at sequence number 4161
nextCrashPoint = 1106	max execution seqeuence number: 4171
nextCrashPoint = 1583	max execution seqeuence number: 4181
Execution 3 at sequence number 4181
nextCrashPoint = 3756	max execution seqeuence number: 4166
nextCrashPoint = 31	max execution seqeuence number: 4176
Execution 4 at sequence number 4176
nextCrashPoint = 2400	max execution seqeuence number: 4181

...

******* Model-checking complete: *******
Number of complete, bug-free executions: 10
Number of buggy executions: 0
Total executions: 10
```

## Supported tests

### For primitives

- `checkpoint`
- `detectable_cas`

### For data structures

- `queue_O0`: ***MSQ-mmt-O0*** in the paper
- `queue_O1`: ***MSQ-mmt-O1*** in the paper
- `queue_O2`: ***MSQ-mmt-O2*** in the paper
- `queue_comb` ***CombQ-mmt***in the paper
- `treiber_stack`: ***TreiberS-mmt*** in the paper
- `list`: ***List-mmt***
- `clevel`: ***Clevel-mmt*** in the paper
