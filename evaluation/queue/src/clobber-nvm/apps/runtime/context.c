#include <stdint.h>
#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "clobber.h"
/*
 * 1. Thread Context Management
 */

void *basePtr = NULL;
uint64_t threadCount = 0;
#ifdef NVM_STATS
static uint64_t bytesWritten = 0;
static uint64_t totalMallocs = 0;
static uint64_t totalFrees = 0;
#endif


#define ContextHash(t) ({ \
    uint64_t h = (uint64_t)t; \
    h ^= h >> 33; \
    h *= 0xff51afd7ed558ccdL; \
    h ^= h >> 33; \
    h *= 0xc4ceb9fe1a85ec53L; \
    h ^= h >> 33; \
    h; \
})

ThreadContext contextMap[MaxThreads];
__thread ThreadContext *myContext = NULL;


void create_context(pthread_t self) {
    uint64_t idx = ContextHash(self);
    uint64_t threadIndex = threadCount;
    threadCount++;
    for (uint64_t i = 0; i < MaxThreads; i++) {
        ThreadContext *ctx = &contextMap[(idx + i) % MaxThreads];
        if (ctx->id != 0) continue;
        if (__sync_bool_compare_and_swap(&ctx->id, 0, (uint64_t)self)) {

	    void* ptr = pmem_tx_alloc(funcPtrSize);
	    ctx->funcPtrOffset = (uint64_t)ptr - (uint64_t)basePtr;
	    ctx->funcPtr = (uint64_t)ptr;
	    ctx->v_Buffer = (uint64_t)malloc(funcPtrSize);

	    add_node(ctx->funcPtrOffset);
            ctx->locksHeld = 0;
            ctx->bytesAllocated = 0;
            ctx->openTxs = 0;
            ctx->bytesWritten = 0;
            ctx->mallocs = 0;
            ctx->frees = 0;
	    ctx->index = threadIndex;
            return;
        }
    }
}


void create_init_funcptr(pthread_t self){
        ThreadContext *ctx = my_context();
        void* ptr = pmem_tx_alloc(funcPtrSize);
        ctx->funcPtrOffset = (uint64_t)ptr - (uint64_t)basePtr;
        ctx->funcPtr = (uint64_t)ptr;
	ctx->v_Buffer = (uint64_t)malloc(funcPtrSize);

        add_node(ctx->funcPtrOffset);
}


void create_init_context(pthread_t self) {
    uint64_t idx = ContextHash(self);
    for (uint64_t i = 0; i < MaxThreads; i++) {
        ThreadContext *ctx = &contextMap[(idx + i) % MaxThreads];
        if (ctx->id != 0) continue;
            if (__sync_bool_compare_and_swap(&ctx->id, 0, (uint64_t)self)) {


            ctx->locksHeld = 0;
            ctx->bytesAllocated = 0;
            ctx->openTxs = 0;
            ctx->bytesWritten = 0;
            ctx->mallocs = 0;
            ctx->frees = 0;
            return;
        }
    }
}


ThreadContext *get_context(pthread_t self) {
    uint64_t idx = ContextHash(self);
    for (uint64_t i = 0; i < MaxThreads; i++) {
        ThreadContext *ctx = &contextMap[(idx + i) % MaxThreads];
        if (ctx->id == (uint64_t)self) return ctx;
    }
    return NULL;
}

ThreadContext *my_context() {
    if (myContext != NULL) return myContext;
    myContext = get_context(pthread_self());
    assert(myContext != NULL);
    return myContext;
}

#ifdef CUSTOM_PTHREAD_CREATE
int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
        void *(*start_routine) (void *), void *arg);
int custom_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
        void *(*start_routine) (void *), void *arg) {
    int s = __wrap_pthread_create(thread, attr, start_routine, arg);
#else
int __real_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                                  void *(*start_routine) (void *), void *arg);
int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
        void *(*start_routine) (void *), void *arg) {
    int s = __real_pthread_create(thread, attr, start_routine, arg);
#endif
    if (s == 0) create_context(*thread);

    return s;
}

int __real_pthread_join(pthread_t thread, void **retval);
int __wrap_pthread_join(pthread_t thread, void **retval) {
    int s = __real_pthread_join(thread, retval);
    if (s == 0) {
        ThreadContext *ctx = get_context(thread);
#ifdef NVM_STATS
        fprintf(stdout, "%zu\t%d active locks and allocated %d bytes\n",
                (uint64_t)thread, ctx->locksHeld, ctx->bytesAllocated);
        __sync_fetch_and_add(&bytesWritten, ctx->bytesWritten);
        __sync_fetch_and_add(&totalMallocs, ctx->mallocs);
        __sync_fetch_and_add(&totalFrees, ctx->frees);
#endif
        ctx->id = 0;
	ctx->index = 0;
	threadCount--;
    }
    return s;
}

/*
 * 2. Hooks callbacks
 */

/* Current version of memcached does not support pointer swizzling.
 * Use absolute pointer instead.
 * Fix the offset if the pmem region size is different.
 */

int is_nvmm(void *ptr) {
    uint64_t offset = (uint64_t)ptr - (uint64_t)basePtr;
    if((offset>0)&&(offset<4294967295))
	return 1;
    else
	return 0;

    return IS_NVMM(ptr) ? 1 : 0;
}

void *to_absolute_ptr(void *ptr) {
    return ptr;

    void* absptr = ABS_PTR(void, ptr);
    return absptr;
}

size_t nvmm_strlen(void *ptr) {
    if (is_nvmm(ptr)) ptr = to_absolute_ptr(ptr);
    return strlen((const char *)ptr);
}

int nvmm_strcmp(const char *str1, const char *str2) {
    char *ptr1 = (char *)str1;
    char *ptr2 = (char *)str2;
    if (is_nvmm(ptr1)) ptr1 = (char *)to_absolute_ptr(ptr1);
    if (is_nvmm(ptr2)) ptr2 = (char *)to_absolute_ptr(ptr2);
    return strcmp(ptr1, ptr2);
}

int nvmm_memcmp(const void *addr1, const void *addr2, size_t num) {
    void *ptr1 = (void *)addr1;
    void *ptr2 = (void *)addr2;
    if (is_nvmm(ptr1)) ptr1 = to_absolute_ptr(ptr1);
    if (is_nvmm(ptr2)) ptr2 = to_absolute_ptr(ptr2);

    return memcmp(ptr1, ptr2, num);
}


int tx_lock(){
    ThreadContext *ctx = my_context();
    if (ctx->openTxs == 0) {
        tx_open(ctx);
        ctx->openTxs++;
    }

    return 0;
}

int tx_unlock(){
    ThreadContext *ctx = my_context();
    if (ctx->openTxs > 0) {
        tx_commit(ctx);
        ctx->openTxs--;
    }
    return 0;
}

void* pmem_tx_free(void* ptr);
void pfree(void *ptr) {
    if ((uint64_t)ptr & PMemBoundary) {
#ifdef NVM_STATS
        ThreadContext *ctx = my_context();
        ctx->frees++;
#endif
        void *nativePtr = ABS_PTR(void, ptr);
        assert(nativePtr != basePtr);
        pmem_tx_free(nativePtr);
    }
    else free(ptr);
}


void *pmalloc(size_t size) {

	//return malloc(size);

    void* ptr = pmem_tx_alloc(size);
    assert((uintptr_t)ptr >= (uintptr_t)basePtr);
    if (ptr == NULL) return NULL;


	return ptr;

    uint64_t offset = (uint64_t)ptr - (uint64_t)basePtr;
    debug("%s: 0x%" PRIx64 "\n", "offset", offset);
    offset |= PMemBoundary;
    debug("%s: 0x%" PRIx64 "\n", "swizzled", offset);

	//printf("base ptr = %p, ptr = %p \n", basePtr, ptr);
	//printf("offset = %p, size = %d \n", offset, size);
    return (void *)offset;
}

/*
 * 5. Constructor and Destructor
 */
void __attribute__((constructor)) init_context() {
    assert(sizeof(ThreadContext) == 96);
    memset(contextMap, 0, sizeof(ThreadContext) * MaxThreads);
    create_init_context(pthread_self());
    basePtr = init_runtime();
    assert(basePtr != NULL);
    create_init_funcptr(pthread_self());
}

void __attribute__((destructor)) finalize_context() {
    finalize_runtime();
#ifdef NVM_STATS
    ThreadContext *ctx = my_context(); // main thread
    assert(ctx != NULL);
    __sync_fetch_and_add(&bytesWritten, ctx->bytesWritten);
    __sync_fetch_and_add(&totalMallocs, ctx->mallocs);
    __sync_fetch_and_add(&totalFrees, ctx->frees);

    fprintf(stdout, "Bytes written:   %zu\n", bytesWritten);
    fprintf(stdout, "PMem allocs:     %zu\n", totalMallocs);
    fprintf(stdout, "PMem frees:      %zu\n", totalFrees);
    fprintf(stdout, "Loop checks:     %zu\n", loopChecks);
#endif
}
