#ifndef BENCHMARK_H
#define BENCHMARK_H

#define MAX_KEY_LENGTH 32

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

bool ifload();

#ifndef PERSISTENT

#include "rbtree.h"
#define RbtreePtrType             rbtree*
#define RbtreePtrAssign(a, b)     a = b
#define RbtreeCreate(tree)        rbtreeCreate(&tree)
#define RbtreeDestroy(tree)       rbtreeDestroy(&tree)


#else

#ifndef PMDK

#include "rbtree.h"
#define RbtreePtrType             rbtree*
#define RbtreePtrAssign(a, b)     a = b
#define RbtreeCreate(tree)        PersistentRbtreeCreate(&tree)
#define RbtreeDestroy(tree)       PersistentRbtreeDestroy(&tree)

void PersistentRbtreeCreate(rbtree **);
void PersistentRbtreeDestroy(rbtree **);

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

