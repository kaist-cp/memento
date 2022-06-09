#include <libpmemobj.h>
#include "benchmark.h"
#include <hashmap_tx.h>
static PMEMobjpool *pop;

int hashmap_recover(struct hashmap_root **rootPtr) {
    pop = pmemobj_open(PMEM_PATH, POBJ_LAYOUT_NAME(HASHMAP));
    if (pop == NULL) return 1;
    PMEMoid root = pmemobj_root(pop, sizeof(struct hashmap_root));
    *rootPtr = pmemobj_direct(root);
    return 0;
}

int hashmap_create(struct hashmap_root **rootPtr) {
    pop = pmemobj_create(PMEM_PATH, POBJ_LAYOUT_NAME(HASHMAP),
            PMEM_SIZE, 0666);
    if (pop == NULL) return 1;

    PMEMoid root = pmemobj_root(pop, sizeof(struct hashmap_root));
    *rootPtr = pmemobj_direct(root);
    for (size_t i = 0; i < BUCKETS; i++) {
        int t = hm_tx_create(pop, &((*rootPtr)->hash[i]), NULL);
        if (t != 0) return 2;
        t = hm_tx_init(pop, (*rootPtr)->hash[i]);
        if (t != 0) return 3;
        PMEMoid oid;
        pmemobj_zalloc(pop, &oid, sizeof(PMEMrwlock), TOID_TYPE_NUM(PMEMrwlock));
        TOID_ASSIGN((*rootPtr)->lock[i], oid);
        pmemobj_rwlock_zero(pop, D_RW((*rootPtr)->lock[i]));
    }

    return 0;
}

void hashmap_close(struct hashmap_root *rootPtr) {
    pmemobj_close(pop);
    pop = NULL;
}

size_t hashmap_size(struct hashmap_root *rootPtr) {
    size_t r = 0;
    for (size_t i = 0; i < BUCKETS; i++) {
        r += hm_tx_count(pop, rootPtr->hash[i]);
    }
    return r;
}

void doInsert(struct hashmap_root *rootPtr, char *key, char *value) {
    uint64_t k = hash((unsigned char*)key);

    TX_BEGIN(pop) {
        PMEMrwlock *lock = D_RW(rootPtr->lock[k % BUCKETS]);
        int t = pmemobj_rwlock_wrlock(pop, lock);
        assert(t == 0);
        PMEMoid d = pmemobj_tx_alloc(KEY_SIZE + strlen(value) + 1,
                TOID_TYPE_NUM(struct hashmap_data));
        struct hashmap_data *dPtr = pmemobj_direct(d);
        memcpy(dPtr->key, key, KEY_SIZE);
        strcpy(dPtr->value, value);
        t = hm_tx_insert(pop, rootPtr->hash[k % BUCKETS], k, d);
        assert(t == 0);
        t = pmemobj_rwlock_unlock(pop, lock);
        assert(t == 0);
    } TX_END
}

void doUpdate(struct hashmap_root *rootPtr, char *key, char *newValue) {
    uint64_t k = hash((unsigned char*)key);

    TX_BEGIN(pop) {
        PMEMrwlock *lock = D_RW(rootPtr->lock[k % BUCKETS]);
        int t = pmemobj_rwlock_wrlock(pop, lock);
        assert(t == 0);
        PMEMoid oid = hm_tx_get(pop, rootPtr->hash[k % BUCKETS], k);
        if (!OID_IS_NULL(oid)) {
            struct hashmap_data *dPtr = pmemobj_direct(oid);
	    pmemobj_tx_add_range_direct(dPtr->value, strlen(dPtr->value)+1);
            strcpy(dPtr->value, newValue);
        }


        t = pmemobj_rwlock_unlock(pop, lock);
        assert(t == 0);
    } TX_END
}

void doRead(struct hashmap_root *rootPtr, char *key, char *buffer) {
    uint64_t k = hash((unsigned char*)key);

    PMEMrwlock *lock = D_RW(rootPtr->lock[k % BUCKETS]);
    int t = pmemobj_rwlock_rdlock(pop, lock);
    assert(t == 0);
    PMEMoid oid = hm_tx_get(pop, rootPtr->hash[k % BUCKETS], k);
    if (!OID_IS_NULL(oid)) {
        struct hashmap_data *dPtr = pmemobj_direct(oid);
        strcpy(buffer, dPtr->value);
    }
    t = pmemobj_rwlock_unlock(pop, lock);
    assert(t == 0);
}
