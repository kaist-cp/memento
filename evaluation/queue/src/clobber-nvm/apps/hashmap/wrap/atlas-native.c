#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <assert.h>
#include <fcntl.h>
#include <pthread.h>

static uint32_t regionId;

void NVM_Initialize();
void NVM_Finalize();
void NVM_CloseRegion(uint32_t);
uint32_t NVM_FindOrCreateRegion(const char*, int, int *);

int __real_main(int, char **);
int __wrap_main(int argc, char **argv) {
    NVM_Initialize();
    int isCreated;
    regionId = NVM_FindOrCreateRegion("AtlasNative", O_RDWR, &isCreated);
    assert(isCreated);

    int r =__real_main(argc, argv);

    NVM_CloseRegion(regionId);
    NVM_Finalize();

    return r;
}

static __thread int64_t locksHeld = 0;

int __real_pthread_mutex_lock(pthread_mutex_t *);
int __wrap_pthread_mutex_lock(pthread_mutex_t *lock) {
    locksHeld++;
    //printf("lock(%p)\n", lock);
    return __real_pthread_mutex_lock(lock);
}

int __real_pthread_mutex_unlock(pthread_mutex_t *);
int __wrap_pthread_mutex_unlock(pthread_mutex_t *lock) {
    locksHeld--;
    //printf("unlock(%p)\n", lock);
    return __real_pthread_mutex_unlock(lock);
}

int __real_pthread_rwlock_rdlock(pthread_rwlock_t *);
int __wrap_pthread_rwlock_rdlock(pthread_rwlock_t *lock) {
    locksHeld++;
    return __real_pthread_rwlock_rdlock(lock);
}

int __real_pthread_rwlock_wrlock(pthread_rwlock_t *);
int __wrap_pthread_rwlock_wrlock(pthread_rwlock_t *lock) {
    locksHeld++;
    return __real_pthread_rwlock_wrlock(lock);
}

int __real_pthread_rwlock_unlock(pthread_rwlock_t *);
int __wrap_pthread_rwlock_unlock(pthread_rwlock_t *lock) {
    locksHeld--;
    return __real_pthread_rwlock_unlock(lock);
}

void *nvm_alloc(size_t, uint32_t);
void *__real_malloc(size_t);
void *__wrap_malloc(size_t size) {
    if (locksHeld == 0) return __real_malloc(size);
    return nvm_alloc(size, regionId);
}

void *nvm_calloc(size_t, size_t, uint32_t);
void *__real_calloc(size_t, size_t);
void *__wrap_calloc(size_t num, size_t size) {
    if (locksHeld == 0) return __real_calloc(num, size);
    return nvm_calloc(num, size, regionId);
}

void *__real_memalign(size_t, size_t);
void *__wrap_memalign(size_t alignment, size_t size) {
    if (locksHeld == 0) return __real_memalign(alignment, size);
    return __wrap_malloc(size);
}

void nvm_free(void *);
void __real_free(void *);
void __wrap_free(void *ptr) {
    if (locksHeld == 0) __real_free(ptr);
    else nvm_free(ptr);
}
