#ifndef TREE_COMMON_H
#define TREE_COMMON_H
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <libpmemobj.h>

#define TREE_BRANCH_FACTOR              32
#define TREE_MAX_KEY_LENGTH             32
#define TREE_MAX_HEIGHT                 5
#define TREE_NIL                        ((uint32_t) - 1)
#define TREE_ROOT                       ((uint32_t) 0)
#define CACHE_LINE_SIZE                 64

typedef char KeyType[TREE_MAX_KEY_LENGTH];

typedef enum {
    Success = 0,
    Failed = 1,
    Exists = 2,
    NotFound = 3,
    TryAgain = 4
} status_t;

#endif // TREE_COMMON_H
