
# Evaluation of "Composing Detectably Persistent Lock-Free Data Structures"

We implemented several lock-free data structures based on memento, and evaluate their correcntess and performance. To reproduce our results, please see the documentation for each data structrue. We assume you mount your PMEM at `/mnt/pmem0/`.
- Correctness Evaluation
    - [Thread crash simulation](./correctness/README.md)
- Performance Evaluation
    - [CAS](./cas/README.md)
    - [List](./list/README.md)
    - [Queue](./queue/README.md)
    - [Hash](./hash/README.md)

