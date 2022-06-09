#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <dlfcn.h>
#include <pthread.h> // for pthread_mutex_t only

#include <time.h>
#include <semaphore.h>
#include <stdbool.h>
#include <errno.h>
#include <assert.h>
#include <signal.h>

#include "txlock.h"
#include "txutil.h"
#include "txcond.h"

_Static_assert(sizeof(txlock_t) == sizeof(pthread_mutex_t), "must be same size as pthreads for drop in replacement");
_Static_assert(sizeof(txcond_t) == sizeof(pthread_cond_t), "must be same size as pthreads for drop in replacement");

// Library search paths
#if defined(__powerpc__) || defined(__powerpc64__)
static const char* LIBPTHREAD_PATH = "/lib/powerpc64le-linux-gnu/libpthread.so.0";
#else
static const char* LIBPTHREAD_PATH = "libpthread.so.0"; // without specifying the path dlopen will search for the library on LIB_PATH
#endif
static void *libpthread_handle = 0;



// Function pointers used to dispatch lock methods
typedef int (*txlock_func_t)(txlock_t *);
typedef int (*txlock_func_alloc_t)(txlock_t **);
static txlock_func_t func_tl_lock = 0;
static txlock_func_t func_tl_trylock = 0;
static txlock_func_t func_tl_unlock = 0;

// txlock interface, dispatches to above function
// pointers
int tl_lock(txlock_t *l) { return func_tl_lock(l); }
int tl_trylock(txlock_t *l) { return func_tl_trylock(l); }
int tl_unlock(txlock_t *l) { return func_tl_unlock(l); }


// Function pointers back into libpthreads implementations
// (these are set on library load)
//typedef int (*fun_pthread_mutex_init_t)(pthread_mutex_t*, const pthread_mutexattr_t*);
//static fun_pthread_mutex_init_t libpthread_mutex_init = 0;
static txlock_func_t libpthread_mutex_lock = 0;
static txlock_func_t libpthread_mutex_trylock = 0;
static txlock_func_t libpthread_mutex_unlock = 0;
static void (*libpthread_exit)(void *) = 0;
static int (*libpthread_create)(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine) (void *), void *arg) = 0;



// test-and-set lock =========================
//
struct _tas_lock_t {
    union{
        struct{
            volatile int32_t val;
            volatile int16_t ready;
            volatile int16_t cnt;
        };
        volatile int64_t all;
    };

} __attribute__((__packed__));

typedef struct _tas_lock_t tas_lock_t;

inline int tatas(volatile int32_t* val, int32_t v) {
    return *val || __sync_lock_test_and_set(val, v);
}

static int tas_lock(tas_lock_t *l) {
    TM_STATS_ADD(my_tm_stats->locks, 1);
    if (tatas(&l->val, 1)) {
        int s = spin_begin();
        do {
            s = spin_wait(s);
        } while (tatas(&l->val, 1));
    }
    TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
    return 0;
}

static int tas_trylock(tas_lock_t *l) {
    if(tatas(&l->val, 1) == 0){
        TM_STATS_ADD(my_tm_stats->locks, 1);
        TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
        return 0;
    }
    return 1;
}

static int tas_unlock(tas_lock_t *l) {
    __sync_lock_release(&l->val);
    TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
    return 0;
}


// test-and-set TM lock =========================
//

static int tas_lock_hle(tas_lock_t *l) {
  int tries = 0;
  int s = spin_begin();

  while (enter_htm(0)) {
    tries++;

    if(tries>=TK_NUM_TRIES){
      TM_STATS_ADD(my_tm_stats->locks, 1);
      while (tatas(&l->val, 1)){s = spin_wait(s);}
      break;
    } else {
      s = spin_wait(s);
    }
  }

  // locked by other thread, waiting for abort
  if (HTM_IS_ACTIVE() && (l->val==1)) {
    while (1)
     spin_wait(spin_begin());
  }

  return 0;
}

static int tas_trylock_hle(tas_lock_t *l) {
  // TODO: 
  assert(0);
}

static int tas_unlock_hle(tas_lock_t *l) {
  if (HTM_IS_ACTIVE()) { // in htm
    HTM_END();
    TM_STATS_ADD(my_tm_stats->commits, 1);
  } else { // not in HTM
    __sync_lock_release(&l->val);
    TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
  }
  return 0;
}


// test-and-set TM lock =========================
//

static int tas_lock_tm(tas_lock_t *l) {
  int tries = 0;
  if (spec_entry == 0) { // not in HTM
    TM_STATS_ADD(my_tm_stats->locks, 1);
    while (tatas(&l->val, 1)) {
      // if lock is held, start speculating
      if(enter_htm(l)==0){return 0;}
      else{tries++;}
      // fall to the lock if out of tries
      if(tries>=TK_NUM_TRIES){
        int s = spin_begin();
        while (tatas(&l->val, 1)){s = spin_wait(s);}
        break;
      }
    }
  }
  TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
  return 0;
}

static int tas_trylock_tm(tas_lock_t *l) {
  if (spec_entry == 0) { // not in HTM
    if(tatas(&l->val, 1)==0){
      TM_STATS_ADD(my_tm_stats->locks, 1);
      TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
      return 0;
    }
    else{return 1;}
  }
  return 0;
}

static int tas_unlock_tm(tas_lock_t *l) {
  if (spec_entry) { // in htm
  } else { // not in HTM
    __sync_lock_release(&l->val);
    TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
  }
  return 0;
}

// tas priority lock =========================
//

static int tas_priority_lock_tm(tas_lock_t *lk) {
  int tries = 0;
  if (spec_entry == 0) { // not in HTM
    TM_STATS_ADD(my_tm_stats->locks, 1);
    tas_lock_t copy;
    int s = spin_begin();
    while(true){
			copy.all = lk->all;
			if(copy.ready==0){
					if (!tatas(&lk->val, 1)) {
							break;
					}
			}
			if(copy.ready < TK_MAX_DISTANCE-TK_MIN_DISTANCE){
				if(enter_htm(lk)==0){
					//if(lk->val!=1){HTM_ABORT(1);}
					return 0;
				}
				else{
					__sync_fetch_and_add(&lk->ready,1);
					while (tatas(&lk->val, 1)){}
					__sync_fetch_and_add(&lk->ready,-1);
					break;
				}
			}
			/*
			if(copy.cnt < TK_MAX_DISTANCE-TK_MIN_DISTANCE){
				bool tmp = __sync_bool_compare_and_swap(&lk->cnt,copy.cnt,copy.cnt+1);
				//if(tmp == 0){}
				//else 
					if(enter_htm(lk)==0){
					//if(lk->val!=1){HTM_ABORT(1);}
					return 0;
				}
				else{
					//__sync_fetch_and_add(&lk->cnt,-1);
					__sync_fetch_and_add(&lk->ready,1);
					s = spin_begin();
					while (tatas(&lk->val, 1)){s = spin_wait(s);}
					__sync_fetch_and_add(&lk->ready,-1);
					break;
				}
			}*/
			/*
			if(copy.cnt < TK_MAX_DISTANCE-TK_MIN_DISTANCE){
				lk->cnt=1;
				if(enter_htm(lk)==0){
					return 0;
				}
				else{
					lk->ready=1;
					lk->cnt=0;
					while (tatas(&lk->val, 1)){if(lk->ready!=1){lk->ready=1;}}
					lk->ready=0;
					break;
				}
			}*/
			s = spin_wait(s);
    }
  }
  TM_STATS_SUB(my_tm_stats->cycles, rdtsc());
  return 0;
}

static int tas_priority_trylock_tm(tas_lock_t *lk) {
  if (spec_entry == 0) { // not in HTM
    tas_lock_t copy;
    copy.all = lk->all;
    if(copy.ready==0 && (tatas(&lk->val, 1)==0)){
      TM_STATS_ADD(my_tm_stats->locks, 1);
      TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
      return 0;
    }
    else{return 1;}
  }
  return 0;
}

static int tas_priority_unlock_tm(tas_lock_t *l) {
  if (spec_entry == 0) { // not in HTM
    __sync_lock_release(&l->val);
    TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
  }
  return 0;
}

// ticket lock =========================
//

struct _ticket_lock_t {
  volatile uint32_t next;
  volatile uint32_t now;
} __attribute__((__packed__));

typedef struct _ticket_lock_t ticket_lock_t;

static int ticket_lock(ticket_lock_t *l) {
    TM_STATS_ADD(my_tm_stats->locks, 1);
    uint32_t my_ticket = __sync_fetch_and_add(&l->next, 1);
    while (my_ticket != l->now) {
        uint32_t dist = my_ticket - l->now;
        spin_wait(16*dist);
    }
    TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
    return 0;
}

static int ticket_trylock(ticket_lock_t *l) {
    ticket_lock_t t, n;
    t.now = t.next = l->now;
    n.now = t.now;
    n.next = t.next+1;

    if((!(__sync_bool_compare_and_swap((int64_t*)l, *(int64_t*)&t, *(int64_t*)&n))) == 0){
      TM_STATS_ADD(my_tm_stats->locks, 1);
      TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
      return 0;
    }
    else{return 1;}
}

static int ticket_unlock(ticket_lock_t *l) {
    l->now++;
    TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
    return 0;
}

// ticket lock TM =========================
//
//
static int ticket_lock_tm(ticket_lock_t *l) {
    if (spec_entry)
        return 0;

    TM_STATS_ADD(my_tm_stats->locks, 1);
    uint32_t tries = 0;
    uint32_t my_ticket = __sync_fetch_and_add(&l->next, 1);
    while (my_ticket != l->now) {
        uint32_t dist = my_ticket - l->now;
        if (dist <= TK_MAX_DISTANCE && dist >= TK_MIN_DISTANCE && tries < TK_NUM_TRIES) {
            // if lock is held, start speculating
            if(enter_htm(l)==0){
				if(l->now==my_ticket){HTM_ABORT(1);}				
				return 0;
			}
            else{
                spin_wait(8);
                tries++;
            }
        } else {
            spin_wait(16*dist);
        }
    }
    TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
    return 0;
}

static int ticket_trylock_tm(ticket_lock_t *l) {
    if (spec_entry) { // in htm
        // nothing
    } else { // not in HTM
        ticket_trylock(l);
    }
    return 0;
}

static int ticket_unlock_tm(ticket_lock_t *l) {
    if (spec_entry) { // in htm
       //if (spec_entry == l) {
       // HTM_ABORT(7);
       //}
    } else { // not in HTM
        l->now++;
        TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
    }
    return 0;
}


// pthreads =====================
//

static int pthread_lock(void *lk){
    TM_STATS_ADD(my_tm_stats->locks, 1);
    int retval = libpthread_mutex_lock(lk);
    TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
    return retval;
}
static int pthread_trylock(void *lk){
    int retval = libpthread_mutex_trylock(lk);
    if(retval==0){
        TM_STATS_ADD(my_tm_stats->locks, 1);
        TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
    }
    return retval;
}
static int pthread_unlock(void *lk){
    int retval = libpthread_mutex_unlock(lk);
    TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
    return retval;
}


// pthreads TM =========================
//

static int pthread_lock_tm(pthread_mutex_t *l) {
  if (spec_entry){return 0;}

  TM_STATS_ADD(my_tm_stats->locks, 1);
  int tries = 0;
  while (libpthread_mutex_trylock((void*)l) != 0) {
    if(enter_htm(l)==0){return 0;}
    else{tries++;}

    if(tries>=TK_NUM_TRIES){
        libpthread_mutex_lock((void*)l);
        break;
    }
  }
  TM_STATS_SUB(my_tm_stats->cycles, RDTSC());
  return 0;
}

static int pthread_trylock_tm(pthread_mutex_t *l) {
    if (spec_entry) { // in htm
        // nothing
        return 0;
    } else { // not in HTM
        int retval = libpthread_mutex_trylock((void*)l);
        if(retval==0){
            TM_STATS_ADD(my_tm_stats->locks, 1);
            TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
        }
        return retval;
    }
}

static int pthread_unlock_tm(pthread_mutex_t *l) {
    if (spec_entry) { // in htm
       //if (spec_entry == l) {
       // HTM_ABORT(7);
       //}
    } else { // not in HTM
        libpthread_mutex_unlock((void*)l);
        TM_STATS_ADD(my_tm_stats->cycles, RDTSC());
    }
    return 0;
}



// queue lock ================================
struct _mcs_lock_t;

struct _mcs_node_t{
  struct _mcs_node_t* volatile lock_next;
  volatile bool wait;
  volatile bool speculate;
  volatile uint64_t cnt;
  struct _mcs_lock_t* lock;
  struct _mcs_node_t* list_next;
  struct _mcs_node_t* list_prev;
} __attribute__((__packed__));

typedef struct _mcs_node_t mcs_node_t;

static __thread mcs_node_t* my_free_nodes = NULL;
static __thread mcs_node_t* my_used_nodes = NULL;

struct _mcs_lock_t {
  volatile mcs_node_t* tail;
  volatile long now_serving;
} __attribute__((__packed__));

typedef struct _mcs_lock_t mcs_lock_t;

static void alloc_more_nodes(){
  const int NUM_NODES = 8;
  mcs_node_t* nodes = malloc(sizeof(mcs_node_t)*NUM_NODES);
  assert(nodes!=NULL);
  for(int i = 0; i<NUM_NODES; i++){
    nodes[i].list_next = &nodes[i+1];
    nodes[i].list_prev=NULL;
    nodes[i].wait = true;
    nodes[i].speculate = true;
    nodes[i].lock = NULL;
    nodes[i].lock_next=NULL;
    nodes[i].cnt = 0;
  }
  nodes[NUM_NODES-1].list_next=NULL;
  my_free_nodes = nodes;
}

static int inline mcs_lock_common(mcs_lock_t *lk, bool try_lock, bool tm) {
  if (spec_entry){return 0;}

  // get a free node
  mcs_node_t* mine;
  mine = my_free_nodes;
  if(mine == NULL){
    alloc_more_nodes();
    mine = my_free_nodes;
    assert(mine!=NULL);
  }

  // init my qnode
  mine->lock_next = NULL;
  mine->lock = lk;
  mine->wait = true;
  mine->speculate = true;
  mine->cnt = 0;

  // then swap it into the root pointer
  mcs_node_t* pred = NULL;
  if(try_lock){
    if(!__sync_bool_compare_and_swap(&lk->tail, NULL, mine)){
      return 1; // return failure
    }
  }
  else{
    pred = (mcs_node_t*)__sync_lock_test_and_set(&lk->tail, mine);
  }

  // we know we'll use the node, so allocate it
  // by moving node off free list and to used list
  my_free_nodes = mine->list_next;
  mine->list_next = my_used_nodes;
  if(my_used_nodes!=NULL){my_used_nodes->list_prev = mine;}
  my_used_nodes = mine;
  mine->list_prev = NULL;

  // now set my flag, point pred to me, and wait for my flag to be unset
  if (pred != NULL) {
    if(!tm){
      pred->lock_next = mine;
      __sync_synchronize(); // is this barrier needed?
      while (mine->wait) {} // spin
    }
    else{
      // finish enqueing
      pred->lock_next = mine;
      while(pred->cnt==0){} // wait for predecessor to get its count
      __sync_synchronize(); // is this barrier needed?
      long cnt = pred->cnt+1;
      mine->cnt = cnt;
      __sync_synchronize(); // is this barrier needed?

      // decide whether to speculate
      long now_serving_copy = lk->now_serving;
      if(now_serving_copy<cnt-TK_MIN_DISTANCE &&
       now_serving_copy>cnt-TK_MAX_DISTANCE &&
       spec_entry==NULL){
        spec_entry = lk;
        if (HTM_SIMPLE_BEGIN() == HTM_SUCCESSFUL) {
          if(mine->speculate!=true || mine->wait!=true){
            HTM_ABORT(0);
          }
          else{return 0;}
        }
        spec_entry=NULL;
      }
      // finished speculating

      // actually acquire the lock
      while (mine->wait) {}
      __sync_synchronize(); // is this barrier needed?
      assert(lk->now_serving == cnt-1);
      lk->now_serving = cnt;
    }
  }
  else{
    if(tm){
      mine->cnt=lk->now_serving+1;
      lk->now_serving++;
    }
  }

  return 0; // return success
}

static int mcs_lock(mcs_lock_t *lk) {
  return mcs_lock_common(lk,false,false);
}


static int mcs_trylock(mcs_lock_t *lk) {
  return mcs_lock_common(lk,true,false);
}


static inline void dealloc_node(mcs_node_t* mine){
  // move node out of used list
  mine->lock_next = NULL;
  if(mine->list_prev!=NULL){
    mine->list_prev->list_next = mine->list_next;
  }
  else{
    my_used_nodes = mine->list_next;
  }
  if(mine->list_next!=NULL){
    mine->list_next->list_prev = mine->list_prev;
  }

  // and onto free list
  mine->list_next = my_free_nodes;
  my_free_nodes = mine;
}

static inline int mcs_unlock_common(mcs_lock_t *lk, bool tm) {

  // traverse used list to find node
  // (assumes we never hold a lot of locks at once)
  mcs_node_t* mine = my_used_nodes;
  assert(mine!=NULL);
  while(mine->lock!=lk){
    mine = mine->list_next;
    assert(mine!=NULL);
  }

  // if my node is the only one, then if I can zero the lock, do so and I'm
  // done
  if (mine->lock_next == NULL) {
    if (__sync_bool_compare_and_swap(&lk->tail, mine, NULL)){
      dealloc_node(mine);
      return 0;
    }
    // uh-oh, someone arrived while I was zeroing... wait for arriver to
    // initialize, fall out to other case
    while (mine->lock_next == NULL) { } // spin
  }

  // halt speculators
  if(tm){
    mcs_node_t* current = mine->lock_next;
    int dist = 1;
    while(current!=NULL){
      if(dist>=TK_MIN_DISTANCE){
        current->speculate = false;
      }
      if(dist>TK_MAX_DISTANCE){break;}
      current = current->lock_next;
      dist++;
    }
  }
  // wake spinners for speculation?????????


  // if someone is waiting on me; set their flag to let them start
  mine->lock_next->wait = false;

  dealloc_node(mine);

  return 0;
}

static int mcs_unlock(mcs_lock_t *lk) {
  return mcs_unlock_common(lk,false);
}


static int mcs_lock_tm(mcs_lock_t *lk) {
  return mcs_lock_common(lk,false,true);
}


static int mcs_trylock_tm(mcs_lock_t *lk) {
  return mcs_lock_common(lk,true,true);
}

static int mcs_unlock_tm(mcs_lock_t *lk) {
  if(!spec_entry){return mcs_unlock_common(lk,true);}
  else{return 0;}
}


// function dispatch =========================
//

struct _lock_type_t {
    const char *name;
    int lock_size;
    txlock_func_t lock_fun;
    txlock_func_t trylock_fun;
    txlock_func_t unlock_fun;
};
typedef struct _lock_type_t lock_type_t;

static lock_type_t lock_types[] = {
    {"pthread",     sizeof(pthread_mutex_t), (txlock_func_t)pthread_lock, (txlock_func_t)pthread_trylock, (txlock_func_t)pthread_unlock},
    {"pthread_tm",  sizeof(pthread_mutex_t), (txlock_func_t)pthread_lock_tm, (txlock_func_t)pthread_trylock_tm, (txlock_func_t)pthread_unlock_tm},
    {"tas",         sizeof(tas_lock_t), (txlock_func_t)tas_lock, (txlock_func_t)tas_trylock, (txlock_func_t)tas_unlock},
    {"tas_tm",      sizeof(tas_lock_t), (txlock_func_t)tas_lock_tm, (txlock_func_t)tas_trylock_tm, (txlock_func_t)tas_unlock_tm},
    {"tas_priority_tm",      sizeof(tas_lock_t), (txlock_func_t)tas_priority_lock_tm, (txlock_func_t)tas_priority_trylock_tm, (txlock_func_t)tas_priority_unlock_tm},
    {"tas_hle",     sizeof(tas_lock_t), (txlock_func_t)tas_lock_hle, (txlock_func_t)tas_trylock_hle, (txlock_func_t)tas_unlock_hle},
    {"ticket",      sizeof(ticket_lock_t), (txlock_func_t)ticket_lock, (txlock_func_t)ticket_trylock, (txlock_func_t)ticket_unlock},
    {"ticket_tm",   sizeof(ticket_lock_t), (txlock_func_t)ticket_lock_tm, (txlock_func_t)ticket_trylock_tm, (txlock_func_t)ticket_unlock_tm},
    {"mcs",   sizeof(mcs_lock_t), (txlock_func_t)mcs_lock, (txlock_func_t)mcs_trylock, (txlock_func_t)mcs_unlock},
    {"mcs_tm",   sizeof(mcs_lock_t), (txlock_func_t)mcs_lock_tm, (txlock_func_t)mcs_trylock_tm, (txlock_func_t)mcs_unlock_tm}
};

static lock_type_t *using_lock_type = &lock_types[2];

// Dynamically find the libpthread implementations
// and store them before replacing them
static void setup_pthread_funcs() {
    char *error;
    void *handle = dlopen(LIBPTHREAD_PATH, RTLD_LAZY);
    if (!handle) {
       fputs (dlerror(), stderr);
       exit(1);
    }
    libpthread_handle = handle;

    // Find libpthread methods
    libpthread_mutex_lock = (txlock_func_t)dlsym(handle, "pthread_mutex_lock");
    libpthread_mutex_trylock = (txlock_func_t)dlsym(handle, "pthread_mutex_trylock");
    libpthread_mutex_unlock = (txlock_func_t)dlsym(handle, "pthread_mutex_unlock");

    // and store them in the lock_types array
    //lock_types[0].lock_fun = libpthread_mutex_lock;
    //lock_types[0].trylock_fun = libpthread_mutex_trylock;
    //lock_types[0].unlock_fun = libpthread_mutex_unlock;

    // handler for pthread_exit and create
    libpthread_exit = (void (*)(void *))dlsym(handle, "pthread_exit");

    libpthread_create = (int (*)(pthread_t *thread, const pthread_attr_t *attr,
      void *(*start_routine) (void *), void *arg))dlsym(handle, "pthread_create");

    if ((error = dlerror()) != NULL)  {
        fputs(error, stderr);
        exit(1);
    }
}


static void (*old_int_handler)(int signum)=SIG_IGN;

static void sig_int_handler(const int sig) {
    if (old_int_handler != SIG_IGN && old_int_handler != SIG_DFL)
        (*old_int_handler)(sig);
    exit(-1);
}

static void (*old_term_handler)(int signum)=SIG_IGN;
static void sig_term_handler(const int sig) {
    if (old_int_handler != SIG_IGN && old_int_handler != SIG_DFL)
        (*old_int_handler)(sig);
    exit(-1);
}

__attribute__((constructor(201)))  // after tl-pthread.so
static void init_lib_txlock() {
    setup_pthread_funcs();

		tm_stats_head = aligned_alloc(256, sizeof(tm_stats_t)); //calloc(1,sizeof(tm_stats_t));
		memset(tm_stats_head, 0, sizeof(tm_stats_t));
		my_tm_stats = tm_stats_head;
		
    // determine lock type
    const char *type = getenv("LIBTXLOCK_LOCK");
    if (type) {
        for (size_t i=0; i<sizeof(lock_types)/sizeof(lock_type_t); i++) {
            if (strcmp(type, lock_types[i].name) == 0) {
                using_lock_type = &lock_types[i];
                break;
            }
        }
    }

    // set appropriate dispatching functions
    func_tl_lock = using_lock_type->lock_fun;
    func_tl_trylock = using_lock_type->trylock_fun;
    func_tl_unlock = using_lock_type->unlock_fun;

    // read auxiliary arguments
    const char* env;
    if ((env = getenv("LIBTXLOCK_MAX_DISTANCE")) != NULL)
        TK_MAX_DISTANCE=atoi(env);
    if ((env = getenv("LIBTXLOCK_MIN_DISTANCE")) != NULL)
        TK_MIN_DISTANCE=atoi(env);
    if ((env = getenv("LIBTXLOCK_NUM_TRIES")) != NULL)
        TK_NUM_TRIES=atoi(env);

      // notify user of arguments
    fprintf(stderr, "LIBTXLOCK_LOCK: %s\n", using_lock_type->name);
    fflush(stderr);

    // register signal handlers just in case the default ones are active:
    old_int_handler = signal(SIGINT, sig_int_handler);
    old_term_handler =  signal(SIGTERM, sig_term_handler);
}


typedef struct {
    void *(*routine) (void *);
    void* args;
} spawn_struct;

void tl_thread_enter() {
    if (my_tm_stats == 0) {
        // push my stats onto the stack
        my_tm_stats = aligned_alloc(256, sizeof(tm_stats_t)); //calloc(1,sizeof(tm_stats_t));
        memset(my_tm_stats, 0, sizeof(tm_stats_t));
        do {
            my_tm_stats->next = tm_stats_head;
        } while(!__sync_bool_compare_and_swap(&tm_stats_head, my_tm_stats->next, my_tm_stats));
    }
}

int tl_in_spec() {
    return HTM_IS_ACTIVE() && spec_entry;
}

void tl_stop_spec() {
    HTM_ABORT(7);
}

static void* _tl_dummy_thread_main(void *spec){

    // unwrap arguments
    spawn_struct* orig = (spawn_struct*)spec;
    void *(*start_routine) (void *);
    void * args;
    void* ret;

    tl_thread_enter();

    // call the actual desired function
    ret = orig->routine(orig->args);

    // clean up spawn structure
    free(orig);

    return ret;
}

int _tl_pthread_create(void *thread, const void *attr, void *(*start_routine) (void *), void *args){
    // wrap arguments
    spawn_struct* orig = malloc(sizeof(spawn_struct)); // destroyed by spawned thread
    orig->routine = start_routine;
    orig->args = args;
    return libpthread_create(thread,attr,&_tl_dummy_thread_main,orig);
}


__attribute__((destructor))
static void uninit_lib_txlock()
{
    struct _tm_stats_t* volatile curr = tm_stats_head;
    while (curr) {
        tm_stats.cycles += curr->cycles;
        tm_stats.tm_cycles += curr->tm_cycles;
        tm_stats.locks += curr->locks;
        tm_stats.tries += curr->tries;
        tm_stats.stops += curr->stops;
        tm_stats.commits += curr->commits;
        tm_stats.overflows += curr->overflows;
        tm_stats.conflicts += curr->conflicts;
        tm_stats.threads += 1;
        curr = curr->next;
    }


    fprintf(stderr, "LIBTXLOCK_LOCK: %s", using_lock_type->name);
    fprintf(stderr, ", LIBTXLOCK_NUM_TRIES: %d, LIBTXLOCK_MIN_DISTANCE: %d, LIBTXLOCK_MAX_DISTANCE: %d", TK_NUM_TRIES, TK_MIN_DISTANCE, TK_MAX_DISTANCE);
    if (tm_stats.threads==0) {
        fprintf(stderr,"\nWARNING: No threads exited properly! Unable to gather profiling information.  \
Ensure all threads properly terminate using pthread_exit()");
    }
    else{
        fprintf(stderr, "\nLIBTXLOCK stats, threads %d",
            tm_stats.threads);
    }
    if (tm_stats.locks!=0) {
        fprintf(stderr, ", avg_lock_cycles: %ld, locks: %d",
                        (tm_stats.cycles/tm_stats.locks), tm_stats.locks);
    }
    if (tm_stats.tries!=0) {
        fprintf(stderr, ", avg_tm_cycles: %ld, tm_tries: %d, commits: %d, overflows: %d, conflicts: %d, stops: %d",
                        (tm_stats.tm_cycles/tm_stats.tries), tm_stats.tries, tm_stats.commits,
                        tm_stats.overflows, tm_stats.conflicts, tm_stats.stops);
    }
    fprintf(stderr, "\n");
    fflush(stderr);

    if (libpthread_handle)
        dlclose(libpthread_handle);
}

// cond var dispatch

int tc_wait(txcond_t *cv, txlock_t *lk){
#if (USE_PTHREAD_CONDVARS)
	__pthread_cond_wait((void*)cv, (void*)lk);
#else
	txcond_wait(cv,lk);
#endif
}
int tc_timedwait(txcond_t *cv, txlock_t *lk, const struct timespec *abs_timeout){
#if (USE_PTHREAD_CONDVARS)
    __pthread_cond_timedwait((void*)cv, (void*)lk, abs_timeout);
#else
    txcond_timedwait(cv,lk,abs_timeout);
#endif
}
int tc_signal(txcond_t* cv){
#if (USE_PTHREAD_CONDVARS)
    __pthread_cond_signal((void*)cv);
#else
    txcond_signal(cv);
#endif
}
int tc_broadcast(txcond_t* cv){
#if (USE_PTHREAD_CONDVARS)
    __pthread_cond_broadcast((void*)cv);
#else
    txcond_broadcast(cv);
#endif
}
