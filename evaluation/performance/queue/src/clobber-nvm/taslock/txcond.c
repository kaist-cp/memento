#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <dlfcn.h>
#include <pthread.h> // for pthread_mutex_t only



// for cond vars
#include <time.h>
#include <semaphore.h>
#include <stdbool.h>
#include <errno.h>
#include <assert.h>

#include "txlock.h"
#include "txutil.h"


/*

static int replace_libpthread = 0;
void tl_replace_libpthread(int r) {replace_libpthread=r;}

// Generalized interface to txlocks
struct _txlock_t;
typedef struct _txlock_t txlock_t;
int tl_alloc(txlock_t **l);
int tl_free(txlock_t *l);
int tl_lock(txlock_t *l);
int tl_trylock(txlock_t *l);
int tl_unlock(txlock_t *l);

*/


/*
// pthreads condvar interface


int   pthread_cond_destroy(pthread_cond_t *);
int   pthread_cond_init(pthread_cond_t *, const pthread_condattr_t *);

int   pthread_cond_broadcast(pthread_cond_t *);
int   pthread_cond_signal(pthread_cond_t *);
int   pthread_cond_timedwait(pthread_cond_t *,
          pthread_mutex_t *, const struct timespec *);
int   pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);

*/


enum {WAITING, TIMEOUT, AWOKEN};

struct _txcond_node_t {
  struct _txcond_node_t* next;
  struct _txcond_node_t* prev;
  sem_t sem;
  int status;
} __attribute__((__packed__));

typedef struct _txcond_node_t txcond_node_t;

struct _txcond_t {
  txcond_node_t* head;
  txcond_node_t* tail;
  utility_lock_t lk;
  uint32_t cnt;
} __attribute__((__packed__));

typedef struct _txcond_t _txcond_t;

static int _txcond_waitcommon(txcond_t* cond_var, txlock_t* lk, bool timed, const struct timespec *abs_timeout){
  int e;

  _txcond_t* cv = (_txcond_t*)cond_var;

  //printf("tc-wait cv:%x lk:%x ilk:%x\n",cv,lk,&cv->lk);

  // create node
  txcond_node_t* node = malloc(sizeof(txcond_node_t));
  if(!node){assert(false);return -1;}
  e = sem_init(&node->sem, 0, 0);
  if(e!=0){return -1;}
  node->next = NULL;
  node->prev = NULL;
  node->status = WAITING;


  //printf("%d\n",cv->lk);
  // enqueue into cond var queue
  ul_lock(&cv->lk);
  if(cv->tail!=NULL){
    node->prev = cv->tail;
    cv->tail->next = node;
    cv->tail = node;
  }
  else{
    cv->head = node;
    cv->tail = node;
  }
  ul_unlock(&cv->lk);
  //printf("tc-wait-enqueued cv:%x lk:%x\n",cv,lk);
  // release lock now that we're enqueued
  tl_unlock(lk);



  // wait
  while(true){
    if(timed){e = sem_timedwait(&node->sem,abs_timeout);}
    else{e = sem_wait(&node->sem);}
    if(e!=0){
      if(errno==EINTR){continue;}
      else if(errno==EINVAL){assert(false);return -1;}
      else if(timed && errno==ETIMEDOUT){
        if(__sync_bool_compare_and_swap(&node->status,WAITING,TIMEOUT)){
          // we won the race to clean up our node,
          // whoever 'wakes' us up later will clean
          errno = ETIMEDOUT;
          return -1;
        }
        else{
          // our semaphore was posted after
          // we timed out, but we lost the race
          // to clean up our node
          free(node);
          break;
        }
      }
      else{assert(false);return -1;} // unknown error
    }
    else{
      // we were woken up via semaphore
      // race on status to figure out cleaner
      if(__sync_bool_compare_and_swap(&node->status,WAITING,AWOKEN)){break;}
      else{ free(node); break; }
    }
  }

  // we've been woken up, so reacquire the lock
  tl_lock(lk);
  return 0;
}


int txcond_timedwait(txcond_t *cv, txlock_t *lk, const struct timespec *abs_timeout){
  return _txcond_waitcommon(cv,lk,true,abs_timeout);
}
int txcond_wait(txcond_t *cv, txlock_t *lk){
  return _txcond_waitcommon(cv,lk,false,NULL);
}




int txcond_signal(txcond_t* cond_var){
  int e, i;
  txcond_node_t* node;
  _txcond_t* cv;

  cv = (_txcond_t*)cond_var;
  //printf("tc-signal cv:%x ilk:%x\n",cv,&cv->lk);

  // access node
  // tend towards LIFO, but throw in eventual FIFO
  ul_lock(&cv->lk);

  // decide if accessing head or tail
  if(cv->cnt==0){cv->cnt=5;}
  cv->cnt = cv->cnt*1103515245 + 12345;
  i = cv->cnt;

  if(i%10==0){
    if(cv->head == NULL){ ul_unlock(&cv->lk); return 0;}
    node = cv->head;
    cv->head = cv->head->next;
    if(cv->head==NULL){cv->tail=NULL;}
    else{cv->head->prev = NULL;}
  }
  else{
    if(cv->tail == NULL){ ul_unlock(&cv->lk); return 0;}
    node = cv->tail;
    cv->tail = node->prev;
    if(cv->tail==NULL){cv->head=NULL;}
    else{cv->tail->next = NULL;}
  }
  ul_unlock(&cv->lk);

  // awaken waiter
  e = sem_post(&node->sem);
  if(e!=0){assert(false);return -1;}

  // successful awaken, race on GC
  if(!__sync_bool_compare_and_swap(&node->status,WAITING,AWOKEN)){free(node);}

  return 0;
}

int txcond_broadcast(txcond_t* cond_var){

  //printf("tc-broadcast cv:%x ilk:%x\n",cv,&cv->lk);

  _txcond_t* cv;
  int e1, e2;
  txcond_node_t* node;
  txcond_node_t* prev_node;
  cv = (_txcond_t*)cond_var;

  // remove entire list
  ul_lock(&cv->lk);
  if(cv->head == NULL){ ul_unlock(&cv->lk); return 0; }
  else{
    node = cv->head;
    cv->head = NULL;
    cv->tail = NULL;
  }
  ul_unlock(&cv->lk);

  // awaken everyone
  e1 = 0;
  while(node!=NULL){
    e2 = sem_post(&node->sem);
    if(e2!=0 && e1==0){e1 = e2;}
    prev_node = node;
    node = node->next;
    if(!__sync_bool_compare_and_swap(&prev_node->status,WAITING,AWOKEN)){free(prev_node);}
  }

  return e1;
}

