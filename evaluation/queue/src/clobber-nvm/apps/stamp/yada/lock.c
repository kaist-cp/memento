#include <pthread.h>
#include "lock.h"
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;


void tx_lock(){
	pthread_mutex_lock(&lock);
}

void tx_unlock(){
	pthread_mutex_unlock(&lock);
}


