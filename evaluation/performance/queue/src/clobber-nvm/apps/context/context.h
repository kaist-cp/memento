#ifndef CONTEXT_H
#define CONTEXT_H

#include <stdio.h>
#include <inttypes.h>

#define PMemPath            "/mnt/pmem0/eval_queue/clobber_queue.pool.context"
#define PMemSize            ((size_t) 16 << 30)
#define PMemBoundary        0x1000000000000
#define IS_NVMM(ptr)        ((uint64_t)ptr & PMemBoundary)
#define ABS_PTR(type, ptr)  (type *)((uintptr_t)basePtr + ((uint64_t)ptr & (PMemBoundary - 1)))
#define MaxThreads          128
#define funcPtrSize	    	2048*8

#ifdef DEBUG
#define debug(fmt, ...) fprintf(stdout, fmt, __VA_ARGS__)
#else
#define debug(fmt, ...) {}
#endif

#ifndef HANDCRAFTED

typedef struct {
    uint64_t id;
    uint64_t index;
    int32_t locksHeld;
    uint32_t bytesAllocated;
    uint64_t openTxs;
    uint64_t funcPtrOffset; // offset from pop_base of the address that function pointers store at
    uint64_t funcPtr;
    uint64_t v_Buffer;//volatile buffer for coelease arguments

    // debug statistics
    uint64_t bytesWritten; // on_nvmm_write()
    uint64_t mallocs; // persistent allocations
    uint64_t frees; // persistent frees

    uint64_t reserved[2];
} ThreadContext;

ThreadContext *my_context();

void *init_runtime();
void finalize_runtime();
void tx_open(ThreadContext *);
void tx_commit(ThreadContext *);
void *pmem_alloc(size_t);
void *pmem_tx_alloc(size_t);
void pmem_free(void *);
#endif // HANDCRAFTED
#endif // CONTEXT_H
