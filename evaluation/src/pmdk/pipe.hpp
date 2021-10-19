#include "queue.hpp"
#include <libpmemobj++/pool.hpp>
#include "../common.hpp"

using namespace pmem::obj;

struct pipe_root {
   persistent_ptr<queue> q1;
   persistent_ptr<queue> q2;

};

void pipe(pool<pipe_root> pop, persistent_ptr<queue> q1, persistent_ptr<queue> q2);

int get_pipe_nops(std::string filepath, int nr_thread, float duration);
