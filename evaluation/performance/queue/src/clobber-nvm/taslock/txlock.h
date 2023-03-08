#ifndef _TXLOCK_H_
#define _TXLOCK_H_

#include <string.h>
#include <time.h> // for cond vars

#ifdef __cplusplus
extern "C" {
#endif

#define TXLOCK_INITIALIZER {0}
#define TXCOND_INITIALIZER {0}

struct _txlock_t{
	// must be same size as pthreads for drop in replacement
	char data[40];
};
typedef struct _txlock_t txlock_t;

void tl_thread_enter();
int tl_in_spec();
void tl_stop_spec();

int tl_lock(txlock_t *l);
int tl_trylock(txlock_t *l);
int tl_unlock(txlock_t *l);

typedef struct{
	// must be same size as pthreads for drop in replacement
	char data[48];
} txcond_t;

int tc_wait(txcond_t *cv, txlock_t *lk);
int tc_timedwait(txcond_t *cv, txlock_t *lk, const struct timespec *abs_timeout);
int tc_signal(txcond_t* cv);
int tc_broadcast(txcond_t* cv);

#ifdef __cplusplus
}
#endif

#endif
