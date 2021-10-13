#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <iostream>

#include "pipe.hpp"

using namespace std;

void pipe(pool<pipe_root> pop, persistent_ptr<queue> q1, persistent_ptr<queue> q2) {
    try {
        transaction::run(pop, [&] {
            optional<int> v = nullopt;
            while(true) {
                v = q1->pop(pop);
                if (v!=nullopt)
                    break;
            }
            q2->push(pop, *v);
        });
    } catch (pmem::transaction_error e) {
        cout << e.what() << endl;
    }
}

int get_pipe_nops(string filepath, int nr_threads, float duration) {
    remove(filepath.c_str());
    auto pop = pool<pipe_root>::create(filepath, "MY_LAYOUT", (size_t) (256 * 1024 * 1024));
    persistent_ptr<pipe_root> q_manager = pop.root();

    // Allocate inner queues
    transaction::run(pop, [&] {
        q_manager->q1 = make_persistent<queue>();
        q_manager->q2 = make_persistent<queue>();
    });

    cout << "initialize.." << endl;
    persistent_ptr<queue> q1 = q_manager->q1;
    persistent_ptr<queue> q2 = q_manager->q2;
    transaction::run(pop, [&] {
        for (int i=0; i<PIPE_INIT_SIZE; i++) {
            q1->push(pop, i);
        }
    });

    atomic<int> ops(0);
    atomic<bool> end(false);
    std::thread workers[nr_threads];

    // `duration`초 동안 pipe 수행 횟수 카운트
    cout << "create " << nr_threads << " threads" << endl;
    for (int tid = 0; tid < nr_threads; ++tid) {
        workers[tid] = std::thread ([&] {
            while(true) {

                pipe(pop, q1, q2);
                // TODO: 스레드별 ops 계산 후 마지막에 합치기? (pebr 벤치마크 코드 참고)
                ops.fetch_add(1);

                // `duration` 시간 지났으면 break
                // TODO: end 없애기. 메인 스레드가 직접 kill 하는 게 나을듯
                if (end.load()) {
                    break;
                }
            }
        });
    }

    // 메인스레드는 `duration` 시간동안 sleep한 후 "시간 끝났다" 표시
    usleep(SEC_2_MICRO_SEC(duration));
    end.store(true);

    for (int tid = 0; tid < nr_threads; ++tid) {
        workers[tid].join();
    }

    return ops.load();
}
