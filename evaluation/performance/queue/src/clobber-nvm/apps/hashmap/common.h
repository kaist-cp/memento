#ifndef HASHMAP_COMMON_H
#define HASHMAP_COMMON_H
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <libpmemobj.h>

#define MAP_MAX_KEY_LENGTH             32
#define CACHE_LINE_SIZE                 64

typedef char KeyType[MAP_MAX_KEY_LENGTH];

typedef enum {
    Success = 0,
    Failed = 1,
    Exists = 2,
    NotFound = 3,
    TryAgain = 4
} status_t;

#endif // TREE_COMMON_H
