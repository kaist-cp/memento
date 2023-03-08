#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include "../../context/context.h"
#include "../tree.h"
#include "admin_pop.h"
#include <libpmemobj.h>
#include <stdlib.h>

typedef struct BinaryData{
    char data[64];
} BinaryData;

POBJ_LAYOUT_BEGIN(TREE);
POBJ_LAYOUT_ROOT(TREE, BPTree);
POBJ_LAYOUT_TOID(TREE, BinaryData);
POBJ_LAYOUT_TOID(TREE, int);
POBJ_LAYOUT_END(TREE);

static PMEMobjpool *pop = NULL;
static BPTree *popRoot = NULL; // support for only one tree
int pertx_counter = 2;

void *to_absolute_ptr(void *);

void* get_pop_addr(){
    return pop;
}

void* get_root_addr(){
    return popRoot;
}

void add_func_index(uint8_t index){
    ThreadContext *ctx = my_context();
    memcpy((void*)(ctx->funcPtr+1), &index, sizeof(uint8_t));
}



void on_nvmm_write(void *ptr, size_t size) {
    debug("on_nvmm_write(%p, %zu)\n", ptr, size);
#ifdef NVM_STATS
    ThreadContext *ctx = my_context();
    ctx->bytesWritten += size;
#endif
    pmemobj_tx_add_range_direct(ptr, size);
}

void nvm_ptr_record(void *ptr, size_t size){
    ThreadContext *ctx = my_context();
    ptr = to_absolute_ptr(ptr);
    if(ptr!=popRoot){
	memcpy((void*)(ctx->funcPtr+pertx_counter),"$",1);

	uint64_t offset = (uint64_t)ptr-(uint64_t)pop;
	memcpy((void*)(ctx->funcPtr+pertx_counter+1), &offset, size);

	pertx_counter = pertx_counter+size+1;
    }
}



void ptr_para_record(void *ptr, size_t size){
    ThreadContext *ctx = my_context();
    memcpy((void*)(ctx->funcPtr+pertx_counter), &size, sizeof(int));
    memcpy((void*)(ctx->funcPtr+pertx_counter+sizeof(int)), ptr, size);

    pertx_counter = pertx_counter+size+sizeof(int);
}



void on_RAW_write(void *ptr, size_t size) {
    debug("on_nvmm_write(%p, %zu)\n", ptr, size);
#ifdef NVM_STATS
    ThreadContext *ctx = my_context();
    ctx->bytesWritten += size;
#endif
    pmemobj_tx_add_range_direct(ptr, size);
}

void on_nvmm_read(void *ptr, size_t size) {
    debug("on_nvmm_read(%p, %zu)\n", ptr, size);
}


void* init_runtime() {
    init_admin_pop();
    pop = pmemobj_open(PMemPath, POBJ_LAYOUT_NAME(TREE));
    if (pop == NULL) {
        pop = pmemobj_create(PMemPath, POBJ_LAYOUT_NAME(TREE), PMemSize, 0666);
    }
    else { // recover existing data structure
        PMEMoid root = pmemobj_root(pop, sizeof(BPTree));
        popRoot = D_RW((TOID(BPTree))root);
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
//    pmemobj_drain(pop);
//    uint8_t valid = 1;
//    memcpy((void*)(ctx->v_Buffer), &valid, sizeof(uint8_t));
 //   pmemobj_memcpy_persist(pop, (void*)(ctx->funcPtr), (void*)(ctx->v_Buffer), pertx_counter); 

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
    if (popRoot == NULL && sizeof(BPTree) == size) {
        debug("%s\n", "allocating root");
        PMEMoid root = pmemobj_root(pop, sizeof(BPTree));
        debug("%s: (0x%" PRIx64 ", 0x%" PRIx64 ")\n", "root", root.pool_uuid_lo, root.off);
        BPTree *rootPtr = D_RW((TOID(BPTree))root);
        debug("%s: %p (%p)\n", "root pointer", rootPtr, pop);
        if (__sync_bool_compare_and_swap(&popRoot, NULL, rootPtr)) return rootPtr;
    }


    PMEMoid oid = pmemobj_tx_alloc(size, TOID_TYPE_NUM(BinaryData));
    debug("allocated %zu bytes: (0x%" PRIx64 ",0x%" PRIx64 ")\n",
            size, oid.pool_uuid_lo, oid.off);
    assert(OID_IS_NULL(oid) == 0);
    return D_RW((TOID(BPTree))oid);
}

void pmem_free(void* ptr) {
    PMEMoid oid = pmemobj_oid(ptr);
    pmemobj_tx_free(oid);
}

/*
 * application specific -- extract functions
 */
void print_prologue(BPTree* popRoot) {
    fprintf(stdout, "Recovered persistent tree\n");
    fprintf(stdout, "> iNodes\n");
    fprintf(stdout, ">> Pointer: %p\n", popRoot->iNodes);
    fprintf(stdout, ">> Size:    %zu\n", popRoot->iNodesSize);
    fprintf(stdout, ">> Used:    %zu\n", popRoot->iNodesUsed);
    fprintf(stdout, ">> Height:  %zu\n", popRoot->iNodesHeight);
    fprintf(stdout, "> plNodes\n");
    fprintf(stdout, ">> Pointer: %p\n", popRoot->plNodes);
    fprintf(stdout, ">> Size:    %zu\n", popRoot->plNodesSize);
    fprintf(stdout, ">> Used:    %zu\n", popRoot->plNodesUsed);
    fprintf(stdout, "> Last ID:  %zu\n", popRoot->totalNodes);
    fprintf(stdout, "> Leafs\n");
    fprintf(stdout, ">> Pointer: %p\n", popRoot->leafs);

    size_t leafNodesFound = 0;
    TreeLNode *leaf = popRoot->leafs;
    while (leaf != NULL) {
        leafNodesFound++;
        leaf = (TreeLNode *)to_absolute_ptr(leaf);
        leaf = leaf->next;
    }
    fprintf(stdout, ">> Total:   %zu\n", leafNodesFound);
}


status_t PersistentTreeCreate(BPTree **tree) {
    if (popRoot == NULL) { // create
        return TreeCreate(tree);
    }
    else { // return recovered tree
        print_prologue(popRoot);
        *tree = popRoot;
        return Success;
    }
}

status_t PersistentTreeDestroy(BPTree **tree) {
    return Success;
}

