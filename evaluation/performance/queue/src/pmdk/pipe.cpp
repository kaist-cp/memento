#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <iostream>

#include "pipe.hpp"

using namespace std;

void pipe(pool<pipe_root> pop, persistent_ptr<queue> q1, persistent_ptr<queue> q2)
{
    try
    {
        transaction::run(
            pop, [&]
            {
                optional<int> v = nullopt;
                while (true)
                {
                    v = q1->pop(pop);
                    if (v != nullopt)
                        break;
                }
                q2->push(pop, *v); }

        );
    }
    catch (pmem::transaction_error e)
    {
        cout << e.what() << endl;
    }
}

int get_pipe_nops(string filepath, int nr_threads, float duration, int init)
{
    remove(filepath.c_str());
    auto pop = pool<pipe_root>::create(filepath, "MY_LAYOUT", ((size_t)POOL_SIZE));
    persistent_ptr<pipe_root> q_manager = pop.root();

    // Initialize
    transaction::run(
        pop, [&]
        {
            q_manager->q1 = make_persistent<queue>();
            q_manager->q2 = make_persistent<queue>(); });
    persistent_ptr<queue> q1 = q_manager->q1;
    persistent_ptr<queue> q2 = q_manager->q2;
    for (int i = 0; i < init; i++)
    {
        q1->push(pop, i);
    }

    std::thread workers[nr_threads];
    int local_ops[nr_threads];
    int sum_ops = 0;

    for (int tid = 0; tid < nr_threads; tid++)
    {
        workers[tid] = std::thread(
            [](int tid, float duration, int &local_ops, pool<pipe_root> pop, persistent_ptr<queue> q1, persistent_ptr<queue> q2)
            {
                local_ops = 0;
                struct timespec begin, end;
                clock_gettime(CLOCK_REALTIME, &begin);
                while (true)
                {
                    clock_gettime(CLOCK_REALTIME, &end);
                    long elapsed = end.tv_sec - begin.tv_sec;
                    if (duration < elapsed)
                    {
                        break;
                    }

                    pipe(pop, q1, q2);
                    local_ops += 1;
                }
            },
            tid, duration, std::ref(local_ops[tid]), pop, q1, q2);
    }

    for (int tid = 0; tid < nr_threads; ++tid)
    {
        workers[tid].join();
        sum_ops += local_ops[tid];
    }
    return sum_ops;
}
