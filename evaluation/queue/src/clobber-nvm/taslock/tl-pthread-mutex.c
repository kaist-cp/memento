#include "txlock.h"
#include <stdio.h>

// internal handlers, should never be called inside a user app
int _tl_pthread_create(void *thread, const void *attr, void *(*start_routine) (void *), void *arg);
int pthread_mutex_lock(void *mutex) {
    return tl_lock(mutex);
}

int pthread_mutex_trylock(void *mutex) {
    return tl_trylock(mutex);
}
int pthread_mutex_unlock(void *mutex) {
    return tl_unlock(mutex);
}
int pthread_cond_broadcast(void *cond) {
    return tc_broadcast((txcond_t*)cond);
}


int pthread_cond_signal(void *cond) {
    return tc_signal(cond);
}

int pthread_cond_timedwait(void *cond, void *mutex, const struct timespec *timeout) {
    return tc_timedwait(cond, mutex, timeout);
}

int pthread_cond_wait(void *cond, void *mutex) {
    return tc_wait(cond, mutex);
}

int pthread_create(void *thread, const void *attr, void *(*start_routine) (void *), void *arg) {
    return _tl_pthread_create(thread, attr, start_routine, arg);
}
