#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <string.h>
#include <fcntl.h>
#include <stdbool.h>

// <refactor>
#define NVM_REGION_NAME     "AtlasPMemRegion"
#define PMemBoundary    0x1000000000000
#define IS_NVMM(ptr) ((uint64_t)ptr & PMemBoundary)
#define ABS_PTR(type, ptr) (type *)((uintptr_t)basePtr + ((uint64_t)ptr & (PMemBoundary - 1)))
// </refactor>

void *basePtr = NULL;
static __thread int64_t locksHeld;
static uint32_t pRegionId;

/*
 * 1. Hooks and Atlas callbacks
 */
int is_nvmm(void *ptr) {
    return (uint64_t)ptr & PMemBoundary ? 1 : 0;
}

void *to_absolute_ptr(void *ptr) {
    return ABS_PTR(void, ptr);
}

void nvm_store(void *, size_t);
void nvm_memcpy(void *, size_t);
void on_nvmm_write(void *ptr, size_t size) {
    nvm_memcpy(ptr, size);
}

void nvm_psync_acq(void *, size_t);
void post_nvmm_write(void *ptr, size_t size) {
    nvm_psync_acq(ptr, size);
}

void on_nvmm_read(void *ptr, size_t size) {}

size_t nvmm_strlen(void *ptr) {
    if (IS_NVMM(ptr)) ptr = ABS_PTR(void, ptr);
    return strlen(ptr);
}

int nvmm_strcmp(const char *str1, const char *str2) {
    const char *ptr1 = IS_NVMM(str1) ? ABS_PTR(const char, str1) : str1;
    const char *ptr2 = IS_NVMM(str2) ? ABS_PTR(const char, str2) : str2;
    return strcmp(ptr1, ptr2);
}

void nvm_acquire(void *);
int __real_pthread_mutex_lock(pthread_mutex_t *);
int __wrap_pthread_mutex_lock(pthread_mutex_t *lock) {
    if (IS_NVMM(lock)) { // TODO remove once bug #1 is fixed
        lock = ABS_PTR(pthread_mutex_t, lock);
    }
    assert(__real_pthread_mutex_lock(lock) == 0);
    nvm_acquire(lock);
    locksHeld++;
    return 0;
}

void nvm_release(void *);
int __real_pthread_mutex_unlock(pthread_mutex_t *);
int __wrap_pthread_mutex_unlock(pthread_mutex_t *lock) {
    if (IS_NVMM(lock)) { // TODO remove once bug #1 is fixed
        lock = ABS_PTR(pthread_mutex_t, lock);
    }
    nvm_release(lock);
    assert(__real_pthread_mutex_unlock(lock) == 0);
    locksHeld--;
    return 0;
}

int __real_pthread_rwlock_init(pthread_rwlock_t *, const pthread_rwlockattr_t *);
int __wrap_pthread_rwlock_init(pthread_rwlock_t *rwlock,
        const pthread_rwlockattr_t *attr) {
    if (IS_NVMM(rwlock)) {
        rwlock = ABS_PTR(pthread_rwlock_t, rwlock);
    }
    return __real_pthread_rwlock_init(rwlock, attr);
}

void nvm_rwlock_rdlock(void *);
int __real_pthread_rwlock_rdlock(pthread_rwlock_t *);
int __wrap_pthread_rwlock_rdlock(pthread_rwlock_t *lock) {
    if (IS_NVMM(lock)) { // TODO remove once bug #1 is fixed
        lock = ABS_PTR(pthread_rwlock_t, lock);
    }
    assert(__real_pthread_rwlock_rdlock(lock) == 0);
    nvm_rwlock_rdlock(lock);
    locksHeld++;
    return 0;
}

void nvm_rwlock_wrlock(void *);
int __real_pthread_rwlock_wrlock(pthread_rwlock_t *);
int __wrap_pthread_rwlock_wrlock(pthread_rwlock_t *lock) {
    if (IS_NVMM(lock)) { // TODO remove once bug #1 is fixed
        lock = ABS_PTR(pthread_rwlock_t, lock);
    }
    assert(__real_pthread_rwlock_wrlock(lock) == 0);
    nvm_rwlock_wrlock(lock);
    locksHeld++;
    return 0;
}

void nvm_rwlock_unlock(void *);
int __real_pthread_rwlock_unlock(pthread_rwlock_t *);
int __wrap_pthread_rwlock_unlock(pthread_rwlock_t *lock) {
    if (IS_NVMM(lock)) { // TODO remove once bug #1 is fixed
        lock = ABS_PTR(pthread_rwlock_t, lock);
    }
    nvm_rwlock_unlock(lock);
    assert(__real_pthread_rwlock_unlock(lock) == 0);
    locksHeld--;
    return 0;
}

/*
 * 2. Allocation Wrappers
 */
static __thread int64_t locksHeld;
void *start_routine_wrapper(void *arg) {
    locksHeld = 0;
    uintptr_t *args = (uintptr_t *)arg;
    void *(*start_routine)(void *) = (void *(*)(void *))args[0];
    void *ret = start_routine((void *)args[1]);
    assert(locksHeld == 0);
    return ret;
}

int __real_pthread_create(pthread_t *, const pthread_attr_t *,
        void *(*)(void *), void *);
int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
        void *(*start_routine) (void *), void *arg) {
    uintptr_t args[2] = { (uintptr_t)start_routine, (uintptr_t)arg };
    return __real_pthread_create(thread, attr, start_routine_wrapper, args);
}

void *nvm_alloc(size_t, uint32_t);
bool atlas_try_setting_root_ptr(void *, size_t);
void NVM_SetRegionRoot(uint32_t, void *);
void *__real_malloc(size_t);
void *__wrap_malloc(size_t size) {
    if (locksHeld == 0) return __real_malloc(size);
    void *ptr = nvm_alloc(size, pRegionId);
    if (ptr == NULL) return NULL;

    uint64_t rPtr = (uint64_t)ptr - (uint64_t)basePtr;
    rPtr |= PMemBoundary;

    if (atlas_try_setting_root_ptr((void *)rPtr, size)) {
        //NVM_SetRegionRoot(pRegionId, (void *)rPtr);
    }

    return (void *)rPtr;
}

void *nvm_calloc(size_t, size_t, uint32_t);
void *__real_calloc(size_t, size_t);
void *__wrap_calloc(size_t num, size_t size) {
    if (locksHeld == 0) return __real_calloc(num, size);
    void *ptr = nvm_calloc(num, size, pRegionId);
    if (ptr == NULL) return NULL;

    uint64_t rPtr = (uint64_t)ptr - (uint64_t)basePtr;
    rPtr |= PMemBoundary;
    return (void *)rPtr;
}

void *__real_memalign(size_t, size_t);
void *__wrap_memalign(size_t alignment, size_t size) {
    if (locksHeld == 0) return __real_memalign(alignment, size);
    return __wrap_malloc(size);
}

void nvm_free(void *);
void __real_free(void *);
void __wrap_free(void *ptr) {
    if (IS_NVMM(ptr)) {
        ptr = ABS_PTR(void, ptr);
        nvm_free(ptr);
    }
    else __real_free(ptr);
}

/*
 * 3. Constructor and Destructor
 */
void NVM_Initialize();
uint32_t NVM_FindOrCreateRegion(const char *, int, int *);
void *NVM_GetRegionRoot(uint32_t);
void atlas_set_root_ptr(void *);
void init_context() {
    NVM_Initialize();
    int newRegion;
    pRegionId = NVM_FindOrCreateRegion(NVM_REGION_NAME, O_RDWR, &newRegion);
    if (newRegion == 0) atlas_set_root_ptr(NVM_GetRegionRoot(pRegionId));
    basePtr = NULL;
    // TODO set basePtr
}

void NVM_Finalize();
void NVM_CloseRegion(uint32_t);
void finalize_context() {
    NVM_CloseRegion(pRegionId);
    NVM_Finalize();
}
