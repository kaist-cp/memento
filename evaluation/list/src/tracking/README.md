
# Summary

In the PPoPP 2022 paper [1] entitled "Detectable Recovery of Lock-Free Data Structures", we present a generic approach called Tracking for deriving detectably recoverable implementations of many widely-used concurrent data structures. Such implementations are appealing for emerging systems featuring byte-addressable nonvolatile main memory (NVMM), whose persistence allows to efficiently resurrect failed processes after crashes. Detectable recovery ensures that after a crash, every executed operation is able to recover and return a correct response, and that the state of the data structure is not corrupted.

We have applied Tracking to derive detectably recoverable implementations of a linked list, a binary search tree, and an exchanger. Our experimental analysis introduces a new way of analyzing the cost of persistence instructions, not by simply counting them but by separating them into categories based on the impact they have on the performance. The analysis reveals that understanding the actual persistence cost of an algorithm in machines with real NVMM, is more complicated than previously thought, and requires a thorough evaluation, since the impact of different persistence instructions on performance may greatly vary. We consider this analysis to be one of the major contributions of the paper.

Here we provide the code, together with the necessary documentation, for reproducing the experimental results presented in the paper, regarding our algorithms. 

We also provide the code for a recoverable implementation of a linked list based on the capsules transformation [2]. To implement the capsules-based recoverable linked list, we make use of the capsules framework, which has been provided by its authors after private communication with them.

# Updates

An up-to-date version of the code provided here, together with additional recoverable implementations, can be found in our working [GitHub repository](https://github.com/ConcurrentDistributedLab/Tracking).

# Reproduce experimental results

First, run the `figures_compile.sh` script to compile the executables. Then, run the `figures_run.sh` script to produce the results of each figure in [3], regarding our algorithms. The script creates the output files in the `results` directory. Finally, run `python figures_plot.py` to plot the figures.


The folder `Expected Results` contains the expected results and figures for our algorithm (Tracking).

After compiling the executables, you can run your own experiment by calling `./<executable_name> <algorithm_name> [threads_number] [duration(seconds)]`.

# Requirements

- A modern 64-bit machine.
- A recent Linux distribution.
- The g++ (version 4.8.5 or greater) compiler.
- Building requires the development versions of the following packages:
    - `libatomic`
    - `libnuma`
    - `libvmem`, necessary for building the persistent objects.
    - `libpmem`, necessary for building the persistent objects.

# License

This code is provided under the [LGPL-2.1 License](https://github.com/ConcurrentDistributedLab/Tracking/blob/master/LICENSE).

# References

[1] - Hagit Attiya, Ohad Ben-Baruch, Panagiota Fatourou, Danny Hendler, and Eleftherios Kosmas. "Detectable Recovery of Lock-Free Data Structures". ACM SIGPLAN Notices. Principles and Practice of Parallel Programming (PPoPP) 2022.

[2] - Naama Ben-David, Guy E Blelloch, Michal Friedman, and Yuanhao Wei. 2019. Delay-free concurrency on faulty persistent memory. In 31st ACM Symp on Parallelism in Algorithms and Architectures (SPAA). 253–264.

# Funding

Panagiota Fatourou: Supported by the EU Horizon 2020, Marie Sklodowska-Curie project with GA No 101031688.

Eleftherios Kosmas: Co-financed by Greece and the European Union (European Social Fund- ESF) through the Operational Programme «Human Resources Development, Education and Lifelong Learning» in the context of the project “Reinforcement of Postdoctoral Researchers - 2nd Cycle” (MIS-5033021), implemented by the State Scholarships Foundation (IKY).

# Contact

For any further information, please contact: ekosmas (at) csd.uoc.gr, ohadben (at) post.bgu.ac.il.