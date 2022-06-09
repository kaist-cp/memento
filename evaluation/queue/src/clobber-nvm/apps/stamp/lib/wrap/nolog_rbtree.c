#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include "../../../context/context.h"
#include "../rbtree.h"
#include "admin_pop.h"
#include <libpmemobj.h>
#include <stdlib.h>
#include "pmdk.h"

typedef struct BinaryData{
    char data[64];
} BinaryData;

POBJ_LAYOUT_BEGIN(RBTREE);
POBJ_LAYOUT_ROOT(RBTREE, rbtree_t);
POBJ_LAYOUT_TOID(RBTREE, BinaryData);
POBJ_LAYOUT_TOID(RBTREE, int);
POBJ_LAYOUT_END(RBTREE);

static PMEMobjpool *pop = NULL;
static rbtree_t *popRoot = NULL; // support for only one list
int pertx_counter = 2;

void *to_absolute_ptr(void *);

void* get_pop_addr(){
    return pop;
}

void* get_root_addr(){
    return popRoot;
}

void add_func_index(uint8_t index){
}


void on_nvmm_write(void *ptr, size_t size) {
    debug("on_nvmm_write(%p, %zu)\n", ptr, size);
#ifdef NVM_STATS
    ThreadContext *ctx = my_context();
    ctx->bytesWritten += size;
#endif
//    pmemobj_tx_add_range_direct(ptr, size);
}

void nvm_ptr_record(void *ptr, size_t size){
}



void ptr_para_record(void *ptr, size_t size){
}

void on_RAW_write(void *ptr, size_t size) {
    debug("on_nvmm_write(%p, %zu)\n", ptr, size);
#ifdef NVM_STATS
    ThreadContext *ctx = my_context();
    ctx->bytesWritten += size;
#endif
//    pmemobj_tx_add_range_direct(ptr, size);
}

void on_nvmm_read(void *ptr, size_t size) {
    debug("on_nvmm_read(%p, %zu)\n", ptr, size);
}


void* init_runtime() {
    init_admin_pop();
    pop = pmemobj_open("/mnt/pmem0/rbtree.pop", POBJ_LAYOUT_NAME(RBTREE));
    if (pop == NULL) {
        pop = pmemobj_create("/mnt/pmem0/rbtree.pop", POBJ_LAYOUT_NAME(RBTREE), PMemSize, 0666);
    }
    else { // recover existing data structure
        PMEMoid root = pmemobj_root(pop, sizeof(rbtree_t));
        popRoot = D_RW((TOID(rbtree_t))root);
    }
	assert(pop != NULL);

    return pop;
}

void finalize_runtime() {
    pmemobj_close(pop);
    admin_pop_close();
}

void tx_open(ThreadContext *ctx) {
    assert(pmemobj_tx_stage() == TX_STAGE_NONE);
    pmemobj_tx_begin(pop, NULL, TX_PARAM_NONE);
}

void tx_commit(ThreadContext *ctx) {
    pmemobj_tx_commit();
    (void)pmemobj_tx_end();
}


void* pmem_tx_alloc(size_t size){
    pmemobj_tx_begin(pop, NULL, TX_PARAM_NONE);

    void* ptr = pmem_alloc(size);

    pmemobj_tx_commit();
    (void)pmemobj_tx_end();
    return ptr;
}



void* pmem_alloc(size_t size) {
    if (popRoot == NULL && sizeof(rbtree_t) == size) {
        debug("%s\n", "allocating root");
        PMEMoid root = pmemobj_root(pop, sizeof(rbtree_t));
        debug("%s: (0x%" PRIx64 ", 0x%" PRIx64 ")\n", "root", root.pool_uuid_lo, root.off);
        rbtree_t *rootPtr = D_RW((TOID(rbtree_t))root);
        debug("%s: %p (%p)\n", "root pointer", rootPtr, pop);
        if (__sync_bool_compare_and_swap(&popRoot, NULL, rootPtr)) return rootPtr;
    }


    PMEMoid oid = pmemobj_tx_alloc(size, TOID_TYPE_NUM(BinaryData));
    debug("allocated %zu bytes: (0x%" PRIx64 ",0x%" PRIx64 ")\n",
            size, oid.pool_uuid_lo, oid.off);
    assert(OID_IS_NULL(oid) == 0);
	    return D_RW((TOID(rbtree_t))oid);
	}

	void pmem_free(void* ptr) {
	    PMEMoid oid = pmemobj_oid(ptr);
	    pmemobj_tx_free(oid);
	}


