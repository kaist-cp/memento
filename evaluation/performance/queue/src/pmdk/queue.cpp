#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/mutex.hpp>
#include <iostream>

#include "queue.hpp"

using namespace pmem::obj;
using namespace std;

void queue::push(pool_base &pop, uint64_t value)
{
    transaction::run(
        pop, [&]
        {
            auto n = make_persistent<node>();
            n->value = value;
            n->next = nullptr;
            if (head == nullptr && tail == nullptr)
            {
                head = tail = n;
            }
            else
            {
                tail->next = n;
                tail = n;
            } },
        pmutex);
}

std::optional<int> queue::pop(pool_base &pop)
{
    std::optional<int> value = std::nullopt;
    transaction::run(
        pop, [&]
        {
            if (head == nullptr)
            {
                return; // EMPTY
            }
            value = head->value;
            auto next = head->next;
            delete_persistent<node>(head);
            head = next;
            if (head == nullptr)
                tail = nullptr; },
        pmutex);
    return value;
}

void queue::show(void) const
{
    for (auto n = head; n != nullptr; n = n->next)
        std::cout << n->value << " ";
    std::cout << std::endl;
}

bool pick(int prob)
{
    return (rand() % 100) < prob;
}

// pair: {enq; deq;}
void op_pair(pool<queue> pop, persistent_ptr<queue> q, int tid, optional<int> prob)
{
    q->push(pop, tid);
    q->pop(pop);
}

// prob{n}: { n% enq; or (100-n)% deq; }
void op_prob(pool<queue> pop, persistent_ptr<queue> q, int tid, optional<int> prob)
{
    if (pick(prob.value()))
    {
        q->push(pop, tid);
    }
    else
    {
        q->pop(pop);
    }
}

int get_queue_nops(string filepath, int nr_threads, float duration, int init, optional<int> prob)
{
    remove(filepath.c_str());
    auto pop = pool<queue>::create(filepath, "MY_LAYOUT", ((size_t)POOL_SIZE));
    persistent_ptr<queue> q = pop.root();

    // Initailize
    for (int i = 0; i < init; i++)
    {
        q->push(pop, i);
    }

    std::thread workers[nr_threads];
    int local_ops[nr_threads];
    int sum_ops = 0;

    // Count the number of times the op is executed in `duration` seconds
    for (int tid = 0; tid < nr_threads; tid++)
    {
        workers[tid] = std::thread(
            [](
                int tid, float duration, int &local_ops, pool<queue> pop, persistent_ptr<queue> q, optional<int> prob)
            {
                auto op = (!prob.has_value()) ? op_pair : op_prob;

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

                    op(pop, q, tid, prob);
                    local_ops += 1;
                }
            },
            tid, duration, std::ref(local_ops[tid]), pop, q, prob);
    }

    for (int tid = 0; tid < nr_threads; ++tid)
    {
        workers[tid].join();
        sum_ops += local_ops[tid];
    }
    return sum_ops;
}
