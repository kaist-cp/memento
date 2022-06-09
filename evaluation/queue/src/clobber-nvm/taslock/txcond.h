#ifndef TXCOND_H
#define TXCOND_H

#include <pthread.h>
#include "txlock.h"

int __pthread_cond_broadcast (pthread_cond_t *cond);
int __pthread_cond_signal (pthread_cond_t *cond);
int __pthread_cond_wait (pthread_cond_t *cond, pthread_mutex_t *mutex);
int __pthread_cond_timedwait (pthread_cond_t *cond, pthread_mutex_t *mutex, const struct timespec *abstime);

int txcond_timedwait(txcond_t *cv, txlock_t *lk, const struct timespec *abs_timeout);
int txcond_wait(txcond_t *cv, txlock_t *lk);
int txcond_signal(txcond_t* cond_var);
int txcond_broadcast(txcond_t* cond_var);

#endif