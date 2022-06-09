#ifndef BENCHMARK_H
#define BENCHMARK_H

#define MAX_KEY_LENGTH 32

#ifndef PERSISTENT

#include "queue.h"
#define QueuePtrType queue *
#define QueuePtrAssign(a, b) a = b
#define QueueCreate(q) queueCreate(&q)
#define QueueDestroy(q) queueDestroy(&q)

#else

#ifndef PMDK

// #include "skiplist.h"
// #define SkiplistPtrType skiplist *
// #define SkiplistPtrAssign(a, b) a = b
// #define SkiplistCreate(list) PersistentSkiplistCreate(&list)
// #define SkiplistDestroy(list) PersistentSkiplistDestroy(&list)
#include "queue.h"
#define QueuePtrType queue *
#define QueuePtrAssign(a, b) a = b
#define QueueCreate(q) queueCreate(&q)
#define QueueDestroy(q) queueDestroy(&q)

void PersistentQueueCreate(queue **);
void PersistentQueueDestroy(queue **);

/*
#else

#include "handcrafted/tree.h"
#define TreePtrType             TOID(struct BPTree)
#define TreePtrAssign(a, b)     TOID_ASSIGN(a, b.oid)
#define BPTreeCreate(tree)      TreeCreate(&tree)
#define BPTreeDestroy(tree)     {} // TreeDestroy(&tree)

*/
#endif // PMDK

#endif // PERSISTENT

#endif // BENCHMARK_H
