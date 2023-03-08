#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include "../../context/context.h"
#include "../tree.h"
#include "atlas-config.h"

#define NVM_REGION_NAME     "AtlasPMemRegion"

static uint32_t pRegionId;

void nvm_memcpy(void *, size_t);
void on_nvmm_write(void *ptr, size_t size) {
    nvm_memcpy(ptr, size);
}

void on_RAW_write(void *ptr, size_t size) {
    nvm_memcpy(ptr, size);
}

void nvm_psync_acq(void *, size_t);
void post_nvmm_write(void *ptr, size_t size) {
    nvm_psync_acq(ptr, size);
}

void on_nvmm_read(void *ptr, size_t size) { }

void add_func_index(uint8_t index){}

void nvm_ptr_record(void *ptr, size_t size){}

void ptr_para_record(void *ptr, size_t size){}

void *init_runtime() {
    uintptr_t basePtr = (uintptr_t)kPRegionsBase_;
    basePtr += kPRegionSize_;
    return (void *)basePtr;
}

void finalize_runtime() { }

void nvm_begin_durable();
void tx_open(ThreadContext *ctx) {
    nvm_begin_durable();
}

void nvm_end_durable();
void tx_commit(ThreadContext *ctx) {
    nvm_end_durable();
}

void *nvm_alloc(size_t, uint32_t);
void *pmem_alloc(size_t size) {
    void *p = nvm_alloc(size, pRegionId);
    return p;
}

void nvm_free(void *);
void pmem_free(void *ptr) {
    nvm_free(ptr);
}

void NVM_Initialize();
uint32_t NVM_FindOrCreateRegion(const char *, int, int *);
void init_atlas() {
    NVM_Initialize();
    int newRegion;
    pRegionId = NVM_FindOrCreateRegion(NVM_REGION_NAME, O_RDWR, &newRegion);
    assert(newRegion);
}

void NVM_Finalize();
void NVM_CloseRegion(uint32_t);
void finalize_atlas() {
    NVM_CloseRegion(pRegionId);
    NVM_Finalize();
}

#ifndef HOOKS_HASHMAP
status_t PersistentTreeCreate(BPTree **tree) {
    init_atlas();
    return TreeCreate(tree);
}

status_t PersistentTreeDestroy(BPTree **tree) {
    finalize_atlas();
    *tree = NULL;
    return Success;
}
#endif // HOOKS_HASHMAP
