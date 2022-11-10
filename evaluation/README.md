
# Evaluation of "A General Framework for Detectable, Persistent, Lock-Free Data Structures"

We implemented several lock-free data structures based on memento, and evaluate their correcntess and performance. To reproduce our results, please see the documentation for each data structrue. We assume you mount your PMEM at `/mnt/pmem0/`.
- Detecability evaluation: [Thread crash test](./correctness/README.md)
- Performance evaluation: [CAS](./cas/README.md), [List](./list/README.md), [Queue](./queue/README.md), [Hash](./hash/README.md)

