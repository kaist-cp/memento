#include <stdint.h>
#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>

#include <errno.h>

#include "clobber.h"


typedef struct BinaryData{
    char data[64];
} BinaryData;

POBJ_LAYOUT_BEGIN(CLOBBER);
POBJ_LAYOUT_TOID(CLOBBER, BinaryData);
POBJ_LAYOUT_TOID(CLOBBER, int);
POBJ_LAYOUT_END(CLOBBER);

static PMEMobjpool *pop = NULL;
static BinaryData *popRoot = NULL; // support for only one data structure
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
    ThreadContext *ctx = my_context();
    if (ctx->openTxs == 0){
	tx_open(ctx);
	ctx->openTxs++;
    }
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
    ThreadContext *ctx = my_context();
    if (ctx->openTxs == 0){
        tx_open(ctx);
        ctx->openTxs++;
    }
}

void on_nvmm_read(void *ptr, size_t size) {
    debug("on_nvmm_read(%p, %zu)\n", ptr, size);
}

void* init_runtime() {
    init_admin_pop();
    pop = pmemobj_open(PMemPath, POBJ_LAYOUT_NAME(CLOBBER));
    if (pop == NULL) {
        pop = pmemobj_create(PMemPath, POBJ_LAYOUT_NAME(CLOBBER), PMemSize, 0666);
    }
    else { // recover existing data structure
        PMEMoid root = pmemobj_root(pop, sizeof(BinaryData));
        popRoot = D_RW((TOID(BinaryData))root);
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


void* pmem_tx_free(void* ptr){
    pmemobj_tx_begin(pop, NULL, TX_PARAM_NONE);

    pmem_free(ptr);

    pmemobj_tx_commit();
    (void)pmemobj_tx_end();
    return ptr;
}


void* pmem_alloc(size_t size) {

    if (popRoot == NULL) {
        debug("%s\n", "allocating root");
        PMEMoid root = pmemobj_root(pop, sizeof(BinaryData));
        debug("%s: (0x%" PRIx64 ", 0x%" PRIx64 ")\n", "root", root.pool_uuid_lo, root.off);
        BinaryData *rootPtr = D_RW((TOID(BinaryData))root);
        debug("%s: %p (%p)\n", "root pointer", rootPtr, pop);
        if (__sync_bool_compare_and_swap(&popRoot, NULL, rootPtr)) return rootPtr;
    }

    PMEMoid oid = pmemobj_tx_alloc(size, TOID_TYPE_NUM(BinaryData));
    debug("allocated %zu bytes: (0x%" PRIx64 ",0x%" PRIx64 ")\n",
            size, oid.pool_uuid_lo, oid.off);
    assert(OID_IS_NULL(oid) == 0);
    return D_RW((TOID(BinaryData))oid);
}

void pmem_free(void* ptr) {
    PMEMoid oid = pmemobj_oid(ptr);
    pmemobj_tx_free(oid);
}

void* get_baseptr(){
	return admin_pop;
}

void* init_admin_pop(){
	admin_pop = pmemobj_open(admin_path, LAYOUT);
	if (!admin_pop) {
		admin_pop = pmemobj_create("/mnt/pmem0/eval_queue/clobber_queue.pool", "linkedlist", 1073741824, 0777);
		if (!admin_pop) {
			printf("ADMIN POP CREATION - Error: failed to create a pool at %s (%d): %s\n", "/mnt/pmem0/eval_queue/clobber_queue.pool", 1073741824, strerror(errno));
			exit(1);
		}
	}
	root = POBJ_ROOT(admin_pop, struct list_head);
	assert(!TOID_IS_NULL(root));
	return admin_pop;
}


void add_node(uint64_t offset){
	pmemobj_mutex_lock(admin_pop, &D_RW(root)->lock);

	TX_BEGIN(admin_pop) {
		TOID(struct list_elem) tail = D_RW(root)->tail;
		TOID(struct list_elem) node;
		node = TX_ZNEW(struct list_elem);

		D_RW(node)->funcptr_offset = offset;
		TX_ADD(root);

		if (!TOID_IS_NULL(tail)) {
			TX_ADD(tail);
			D_RW(tail)->next = node;
		} else {
			D_RW(root)->head = node;
		}

		D_RW(root)->tail = node;
		D_RW(root)->num_elements++;

	} TX_END

	pmemobj_mutex_unlock(admin_pop, &D_RW(root)->lock);
}



int get_all_elem(uint64_t* offset){
	TOID(struct list_elem) tmp = D_RO(root)->head;
	int i=0;
	while (!TOID_IS_NULL(tmp)) {
		offset[i] = D_RO(tmp)->funcptr_offset;
		i++;
		tmp = D_RO(tmp)->next;
	}
	return i;
}

void scan_all_offset(){
/*
	TOID(struct list_elem) tmp = D_RO(root)->head;
	while (!TOID_IS_NULL(tmp)) {
		uint64_t offset = D_RO(tmp)->funcptr_offset;
		//TODO: recovery code
		printf("%p ", offset);
		tmp = D_RO(tmp)->next;
	}
	printf("\n");
*/
}

void admin_pop_close(){
	// pool deletion
	pmemobj_close(admin_pop);
}

int admin_pop_check(){
	int ret = 0;
	// pool consistency check
	ret = pmemobj_check(admin_path, LAYOUT);
	if (ret == 1) {
		printf("Pool %s is consistent.\n", admin_path);
	} else if (ret == 0) {
		printf("Error: pool is not consistent.\n");
		exit(1);
	} else {
		printf("Error: pmemobj_check failed: %s\n", strerror(errno));
		exit(1);
	}

	return 0;
}

