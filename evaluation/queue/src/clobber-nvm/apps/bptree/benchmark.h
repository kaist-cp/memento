#ifndef BENCHMARK_H
#define BENCHMARK_H

#ifndef PERSISTENT

#include "tree.h"
#define TreePtrType             BPTree*
#define TreePtrAssign(a, b)     a = b
#define BPTreeCreate(tree)      TreeCreate(&tree)
#define BPTreeDestroy(tree)     TreeDestroy(&tree)

#else

#ifndef PMDK

#include "tree.h"
#define TreePtrType             BPTree*
#define TreePtrAssign(a, b)     a = b
#define BPTreeCreate(tree)      PersistentTreeCreate(&tree)
#define BPTreeDestroy(tree)     PersistentTreeDestroy(&tree)

status_t PersistentTreeCreate(BPTree **);
status_t PersistentTreeDestroy(BPTree **);


#endif // PMDK

#endif // PERSISTENT

#endif // BENCHMARK_H

