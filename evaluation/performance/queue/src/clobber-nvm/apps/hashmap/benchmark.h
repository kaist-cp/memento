#ifndef BENCHMARK_H
#define BENCHMARK_H
#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <stdio.h>

#ifdef LIBPMEMOBJ_H
#include <hashmap_tx.h>
#endif // LIBPMEMOBJ_H

#define KEY_SIZE        64
#define BUCKETS         256

struct hashmap_root;
struct hashmap_data;

#ifdef LIBPMEMOBJ_H
#define PMEM_PATH       "/mnt/pmem0/pop.pmdk"
#define PMEM_SIZE       ((size_t)16 << 30)
POBJ_LAYOUT_BEGIN(HASHMAP);
POBJ_LAYOUT_ROOT(HASHMAP, struct hashmap_root);
POBJ_LAYOUT_TOID(HASHMAP, struct hashmap_data);
POBJ_LAYOUT_TOID(HASHMAP, PMEMrwlock);
POBJ_LAYOUT_END(HASHMAP);
#endif // LIBPMEMOBJ_H

struct hashmap_root {
#ifdef LIBPMEMOBJ_H
    TOID(struct hashmap_tx) hash[BUCKETS];
    TOID(PMEMrwlock) lock[BUCKETS];
#else
    struct hashmap_tx *hash[BUCKETS];
#ifdef EXCLUSIVE_LOCKS
    pthread_mutex_t *lock[BUCKETS];
#else
    pthread_rwlock_t *lock[BUCKETS];
#endif // EXCLUSIVE_LOCKS
#endif // LIBPMEMOBJ_H
};

struct hashmap_data {
    char key[KEY_SIZE];
    char value[4];
};

uint64_t hash(unsigned char *str);

#endif // BENCHMARK_H
