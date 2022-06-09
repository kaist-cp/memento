#include "benchmark.h"
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <stdlib.h>
#include <malloc.h>
#include "hashmap_v.h"

int hashmap_recover(struct hashmap_root **rootPtr) {
    // nothing to recover from (volatile hash-map)
    return 1;
}

int hashmap_create(struct hashmap_root **rootPtr) {
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&lock);
    *rootPtr = (struct hashmap_root*)malloc(sizeof(struct hashmap_root));
    for (size_t i = 0; i < BUCKETS; i++) {
        int t = hm_v_create(&((*rootPtr)->hash[i]), NULL);
        assert(t == 0);
        t = hm_v_init((*rootPtr)->hash[i]);
        assert(t == 0);
#ifdef EXCLUSIVE_LOCKS
        (*rootPtr)->lock[i] = (pthread_mutex_t*)memalign(64, sizeof(pthread_mutex_t));
        pthread_mutex_init((*rootPtr)->lock[i], NULL);
#else
        (*rootPtr)->lock[i] = (pthread_rwlock_t*)memalign(64, sizeof(pthread_rwlock_t));
        pthread_rwlock_init((*rootPtr)->lock[i], NULL);
#endif
    }
    pthread_mutex_unlock(&lock);
    return 0;
}

void hashmap_close(struct hashmap_root *rootPtr) {
    dumpstats();
    // nothing to do
}

size_t hashmap_size(struct hashmap_root *rootPtr) {
    size_t r = 0;
    for (size_t i = 0; i < BUCKETS; i++) {
        r += hm_v_count(rootPtr->hash[i]);
    }
    return r;
}

void doInsert(struct hashmap_root *rootPtr, char *key, size_t keysize, char *value, size_t valuesize) {
    uint64_t k = hash((unsigned char*)key);
	struct entry *e = (struct entry*)malloc(sizeof(struct entry));
#ifdef EXCLUSIVE_LOCKS
    pthread_mutex_t *lock = rootPtr->lock[k % BUCKETS];
    assert(pthread_mutex_lock(lock) == 0);
#else
    pthread_rwlock_t *lock = rootPtr->lock[k % BUCKETS];
    assert(pthread_rwlock_wrlock(lock) == 0);
#endif
/*
    size_t dataSize = KEY_SIZE + strlen(value) + 1;
    struct hashmap_data *dPtr = (struct hashmap_data*)malloc(dataSize);
    memcpy(dPtr->key, key, KEY_SIZE);
    strcpy(dPtr->value, value);
    int t = hm_v_insert(rootPtr->hash[k % BUCKETS], e, k, (char *)dPtr);
*/
	int t = hm_v_insert(rootPtr->hash[k % BUCKETS], e, k, value);
    assert(t == 0);
#ifdef EXCLUSIVE_LOCKS
    assert(pthread_mutex_unlock(lock) == 0);
#else
    assert(pthread_rwlock_unlock(lock) == 0);
#endif
}

void doUpdate(struct hashmap_root *rootPtr, char *key, size_t keysize, char *newValue, size_t valuesize) {
    uint64_t k = hash((unsigned char*)key);
#ifdef EXCLUSIVE_LOCKS
    pthread_mutex_t *lock = rootPtr->lock[k % BUCKETS];
    assert(pthread_mutex_lock(lock) == 0);
#else
    pthread_rwlock_t *lock = rootPtr->lock[k % BUCKETS];
    assert(pthread_rwlock_wrlock(lock) == 0);
#endif
    char *v = hm_v_get(rootPtr->hash[k % BUCKETS], k);
    if (v != NULL) {
		strcpy(v, newValue);
//        struct hashmap_data *dPtr = (struct hashmap_data*)v;
//        strcpy(dPtr->value, newValue); // NOTE will call on_nvmm_write
    }
#ifdef EXCLUSIVE_LOCKS
    assert(pthread_mutex_unlock(lock) == 0);
#else
    assert(pthread_rwlock_unlock(lock) == 0);
#endif
}

void doRead(struct hashmap_root *rootPtr, char *key, size_t keysize, char *buffer, size_t buffersize) {
    uint64_t k = hash((unsigned char*)key);

#ifdef EXCLUSIVE_LOCKS
    pthread_mutex_t *lock = rootPtr->lock[k % BUCKETS];
    assert(pthread_mutex_lock(lock) == 0);
#else
    pthread_rwlock_t *lock = rootPtr->lock[k % BUCKETS];
    assert(pthread_rwlock_rdlock(lock) == 0);
#endif
    char *v = hm_v_get(rootPtr->hash[k % BUCKETS], k);
    if (v != NULL) {
		strcpy(buffer, v);
//        struct hashmap_data *dPtr = (struct hashmap_data*)v;
//        strcpy(buffer, dPtr->value); // NOTE will call on_nvmm_write
    }
#ifdef EXCLUSIVE_LOCKS
    assert(pthread_mutex_unlock(lock) == 0);
#else
    assert(pthread_rwlock_unlock(lock) == 0);
#endif
}
