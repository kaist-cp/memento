#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include "../../context/context.h"
#include "../benchmark.h"
#include <libpmemobj.h>
#include "admin_pop.h"

typedef struct {
    char data[64];
} BinaryData;

POBJ_LAYOUT_BEGIN(HASHMAP);
POBJ_LAYOUT_ROOT(HASHMAP, struct hashmap_root);
POBJ_LAYOUT_TOID(HASHMAP, BinaryData);
POBJ_LAYOUT_END(HASHMAP);

PMEMobjpool *pop = NULL;
struct hashmap_root *popRoot = NULL;

int pertx_counter = 2;

void *to_absolute_ptr(void *);

void* get_pop_addr(){
    return pop;
}

void* get_root_addr(){
    return popRoot;
}


void on_nvmm_write(void *ptr, size_t size) {
    debug("on_nvmm_write(%p, %zu)\n", ptr, size);
#ifdef NVM_STATS
    ThreadContext *ctx = my_context();
    ctx->bytesWritten += size;
#endif
//    pmemobj_tx_add_range_direct(ptr, size);
}

void on_RAW_write(void *ptr, size_t size) {
    debug("on_nvmm_write(%p, %zu)\n", ptr, size);
#ifdef NVM_STATS
    ThreadContext *ctx = my_context();
    ctx->bytesWritten += size;
#endif
    pmemobj_tx_add_range_direct(ptr, size);
}

void add_func_index(uint8_t index){
    ThreadContext *ctx = my_context();
    memcpy((void*)(ctx->v_Buffer+1), &index, sizeof(uint8_t));

}


void nvm_ptr_record(void *ptr, size_t size){
    ThreadContext *ctx = my_context();
    ptr = to_absolute_ptr(ptr);
    if(ptr!=popRoot){
        memcpy((void*)(ctx->v_Buffer+pertx_counter),"$",1);
        uint64_t offset = (uint64_t)ptr-(uint64_t)pop;
        memcpy((void*)(ctx->v_Buffer+pertx_counter+1), &offset, size);

        pertx_counter = pertx_counter+size+1;
    }
}



void ptr_para_record(void *ptr, size_t size){
    ThreadContext *ctx = my_context();

    memcpy((void*)(ctx->v_Buffer+pertx_counter), &size, sizeof(int));
    memcpy((void*)(ctx->v_Buffer+pertx_counter+sizeof(int)), ptr, size);

    pertx_counter = pertx_counter+size+sizeof(int);
}



void on_nvmm_read(void *ptr, size_t size) {}

void* init_runtime() {
    init_admin_pop();
    pop = pmemobj_open(PMemPath, POBJ_LAYOUT_NAME(HASHMAP));
    if (pop == NULL) {
        pop = pmemobj_create(PMemPath, POBJ_LAYOUT_NAME(HASHMAP), PMemSize, 0666);
        popRoot = NULL;
    }
    else { // recover existing data structure
        PMEMoid root = pmemobj_root(pop, sizeof(struct hashmap_root));
        popRoot = D_RW((TOID(struct hashmap_root))root);
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
    uint8_t valid = 0;

    memcpy((void*)(ctx->funcPtr), &valid, sizeof(uint8_t));


    pmemobj_tx_commit();
    (void)pmemobj_tx_end();
    pertx_counter = 2;
}


void* pmem_tx_alloc(size_t size){
    pmemobj_tx_begin(pop, NULL, TX_PARAM_NONE);

    void* ptr = pmem_alloc(size);

    pmemobj_tx_commit();
    (void)pmemobj_tx_end();
    return ptr;
}


void* pmem_alloc(size_t size) {
    if (popRoot == NULL && sizeof(struct hashmap_root) == size) {
        debug("%s\n", "allocating root");
        PMEMoid root = pmemobj_root(pop, sizeof(struct hashmap_root));
        debug("%s: (0x%" PRIx64 ", 0x%" PRIx64 ")\n", "root", root.pool_uuid_lo, root.off);
        struct hashmap_root *rootPtr = D_RW((TOID(struct hashmap_root))root);
        debug("%s: %p (%p)\n", "root pointer", rootPtr, pop);
        if (__sync_bool_compare_and_swap(&popRoot, NULL, rootPtr)) return rootPtr;
    }

    PMEMoid oid = pmemobj_tx_alloc(size, TOID_TYPE_NUM(BinaryData));
    debug("allocated %zu bytes: (0x%" PRIx64 ",0x%" PRIx64 ")\n",
            size, oid.pool_uuid_lo, oid.off);
    assert(OID_IS_NULL(oid) == 0);
    return D_RW((TOID(struct hashmap_root))oid);
}

void pmem_free(void* ptr) {
    PMEMoid oid = pmemobj_oid(ptr);
    pmemobj_tx_free(oid);
}

/*
 * application specific -- extract functions
 */
int __wrap_hashmap_recover(struct hashmap_root **ptr) {
    if (popRoot == NULL) return 1;
    *ptr = popRoot;
    fprintf(stdout, "Recovered persistent hash-map\n");
    fprintf(stdout, "Root pointer:\t%p\n", popRoot);
    return 0;
}

void dumpstats(){}
