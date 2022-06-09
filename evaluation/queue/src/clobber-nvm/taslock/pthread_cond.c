#if (!USE_PTHREAD_COND_VARS)

#define _GNU_SOURCE      


#include <endian.h>
#include <errno.h>
//#include <sysdep.h>
//#include <lowlevellock.h>
#include <pthread.h>

//#include <shlib-compat.h>
//#include <kernel-features.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <dlfcn.h>
#include <limits.h>
#include <stdint.h>

#include <linux/futex.h>  
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/time.h>

// link to txlock
#include "txlock.h"
#include "txcond.h"
#include "txutil.h"
#define __pthread_mutex_unlock_usercnt(mutex, cnt) tl_unlock((void*)mutex)
#define __pthread_mutex_cond_lock(mutex)  tl_lock((void*)mutex)



// random patches

/* Mutex types.  */
#define  PTHREAD_MUTEX_NORMAL PTHREAD_MUTEX_TIMED_NP
#define  PTHREAD_MUTEX_RECURSIVE  PTHREAD_MUTEX_RECURSIVE_NP
#define  PTHREAD_MUTEX_ERRORCHECK  PTHREAD_MUTEX_ERRORCHECK_NP
#define  PTHREAD_MUTEX_DEFAULT  PTHREAD_MUTEX_NORMAL





// from atomic.h

# define atomic_exchange_and_add(mem, value) \
  ({ __typeof (*(mem)) __oldval;					      \
     __typeof (mem) __memp = (mem);					      \
     __typeof (*(mem)) __value = (value);				      \
									      \
     do									      \
       __oldval = (*__memp);						      \
     while (__builtin_expect (__sync_bool_compare_and_swap (__memp,   \
								    __oldval  \
								    + __value,\
								    __oldval),\
			      0));					      \
									      \
     __oldval; })


     
# define atomic_bit_test_set(mem, bit) \
  ({ __typeof (*(mem)) __atg14_old;					      \
     __typeof (mem) __atg14_memp = (mem);				      \
     __typeof (*(mem)) __atg14_mask = ((__typeof (*(mem))) 1 << (bit));	      \
									      \
     do									      \
       __atg14_old = (*__atg14_memp);					      \
     while (__builtin_expect						      \
	    (__sync_bool_compare_and_swap (__atg14_memp,	      \
						   __atg14_old | __atg14_mask,\
						   __atg14_old), 0));	      \
									      \
     __atg14_old & __atg14_mask; })
     
     
# define atomic_add_zero(mem, value)					      \
  ({ __typeof (value) __aaz_value = (value);				      \
     atomic_exchange_and_add (mem, __aaz_value) == -__aaz_value; })
     
# define atomic_increment(mem) __sync_fetch_and_add((mem), 1)
# define atomic_decrement(mem) __sync_fetch_and_sub((mem), 1)


// internaltypes.h

/* Copyright (C) 2002, 2003, 2004, 2007 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Ulrich Drepper <drepper@redhat.com>, 2002.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */




struct pthread_attr
{
  /* Scheduler parameters and priority.  */
  struct sched_param schedparam;
  int schedpolicy;
  /* Various flags like detachstate, scope, etc.  */
  int flags;
  /* Size of guard area.  */
  size_t guardsize;
  /* Stack handling.  */
  void *stackaddr;
  size_t stacksize;
  /* Affinity map.  */
  cpu_set_t *cpuset;
  size_t cpusetsize;
};

#define ATTR_FLAG_DETACHSTATE		0x0001
#define ATTR_FLAG_NOTINHERITSCHED	0x0002
#define ATTR_FLAG_SCOPEPROCESS		0x0004
#define ATTR_FLAG_STACKADDR		0x0008
#define ATTR_FLAG_OLDATTR		0x0010
#define ATTR_FLAG_SCHED_SET		0x0020
#define ATTR_FLAG_POLICY_SET		0x0040


/* Mutex attribute data structure.  */
struct pthread_mutexattr
{
  /* Identifier for the kind of mutex.

     Bit 31 is set if the mutex is to be shared between processes.

     Bit 0 to 30 contain one of the PTHREAD_MUTEX_ values to identify
     the type of the mutex.  */
  int mutexkind;
};


/* Conditional variable attribute data structure.  */
struct pthread_condattr
{
  /* Combination of values:

     Bit 0  : flag whether coditional variable will be shareable between
	      processes.

     Bit 1-7: clock ID.  */
  int value;
};


/* The __NWAITERS field is used as a counter and to house the number
   of bits for other purposes.  COND_CLOCK_BITS is the number
   of bits needed to represent the ID of the clock.  COND_NWAITERS_SHIFT
   is the number of bits reserved for other purposes like the clock.  */
#define COND_CLOCK_BITS		1
#define COND_NWAITERS_SHIFT	1


/* Read-write lock variable attribute data structure.  */
struct pthread_rwlockattr
{
  int lockkind;
  int pshared;
};


/* Barrier data structure.  */
struct pthread_barrier
{
  unsigned int curr_event;
  int lock;
  unsigned int left;
  unsigned int init_count;
  int private;
};


/* Barrier variable attribute data structure.  */
struct pthread_barrierattr
{
  int pshared;
};


/* Thread-local data handling.  */
struct pthread_key_struct
{
  /* Sequence numbers.  Even numbers indicated vacant entries.  Note
     that zero is even.  We use uintptr_t to not require padding on
     32- and 64-bit machines.  On 64-bit machines it helps to avoid
     wrapping, too.  */
  uintptr_t seq;

  /* Destructor for the data.  */
  void (*destr) (void *);
};

/* Check whether an entry is unused.  */
#define KEY_UNUSED(p) (((p) & 1) == 0)
/* Check whether a key is usable.  We cannot reuse an allocated key if
   the sequence counter would overflow after the next destroy call.
   This would mean that we potentially free memory for a key with the
   same sequence.  This is *very* unlikely to happen, A program would
   have to create and destroy a key 2^31 times (on 32-bit platforms,
   on 64-bit platforms that would be 2^63).  If it should happen we
   simply don't use this specific key anymore.  */
#define KEY_USABLE(p) (((uintptr_t) (p)) < ((uintptr_t) ((p) + 2)))


/* Handling of read-write lock data.  */
// XXX For now there is only one flag.  Maybe more in future.
#define RWLOCK_RECURSIVE(rwlock) ((rwlock)->__data.__flags != 0)


/* Semaphore variable structure.  */
struct new_sem
{
  unsigned int value;
  int private;
  unsigned long int nwaiters;
};

struct old_sem
{
  unsigned int value;
};




// lowlevellock.h
/* Low level locking macros used in NPTL implementation.  Stub version.
   Copyright (C) 2002, 2007 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Ulrich Drepper <drepper@redhat.com>, 2002.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */
#define FUTEX_WAIT		0
#define FUTEX_WAKE		1
#define FUTEX_REQUEUE		3
#define FUTEX_CMP_REQUEUE	4
#define FUTEX_WAKE_OP		5
#define FUTEX_OP_CLEAR_WAKE_IF_GT_ONE	((4 << 24) | 1)
#define FUTEX_LOCK_PI		6
#define FUTEX_UNLOCK_PI		7
#define FUTEX_TRYLOCK_PI	8
#define FUTEX_WAIT_BITSET	9
#define FUTEX_WAKE_BITSET	10
#define FUTEX_PRIVATE_FLAG	128
#define FUTEX_CLOCK_REALTIME	256

#define FUTEX_BITSET_MATCH_ANY	0xffffffff

/* Values for 'private' parameter of locking macros.  Yes, the
   definition seems to be backwards.  But it is not.  The bit will be
   reversed before passing to the system call.  */
#define LLL_PRIVATE	0
#define LLL_SHARED	FUTEX_PRIVATE_FLAG

/* Initializers for lock.  */
#define LLL_LOCK_INITIALIZER		(0)
#define LLL_LOCK_INITIALIZER_LOCKED	(1)

/* assumes all futex are private */
#  define __lll_private_flag(fl, private) \
  ((fl) | FUTEX_PRIVATE_FLAG)

static long sys_futex(void *addr1, int op, int val1, struct timespec *timeout,
                      void *addr2, int val3) {
    return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}


int lll_futex_timed_wait(int* futex, int val, struct timespec* timeout, int private){
    return sys_futex(futex, __lll_private_flag (FUTEX_WAIT, private), val, timeout, NULL, 0);
}

#define lll_futex_wait(futex, val, private) \
  lll_futex_timed_wait (futex, val, NULL, private)

int lll_futex_wake(int* futex, int nr, int private){
    return sys_futex(futex, __lll_private_flag (FUTEX_WAKE, private), nr, NULL, NULL, 0);
}


// TODO: is this void* cast correct?? it comes from the assembly, but not the documentation.  Ugh.
int lll_futex_wake_unlock(int* futex, int nr_wake, long int nr_wake2, int* futex2, int private){
    return sys_futex(futex, __lll_private_flag (FUTEX_WAKE_OP, private), nr_wake, (void*) nr_wake2, futex2, (int) FUTEX_OP_CLEAR_WAKE_IF_GT_ONE);
}

// TODO: is this void* cast correct?? it comes from the assembly, but not the documentation.  Ugh.
int lll_futex_requeue(int* futex, int nr_wake, long int nr_move, int* mutex, int val, int private){
    return sys_futex(futex, __lll_private_flag (FUTEX_CMP_REQUEUE, private), nr_wake, (void*) nr_move, mutex, val);
}

static inline void
__generic_mutex_lock (int *mutex, int private)
{
  unsigned int v;

  /* Bit 31 was clear, we got the mutex.  (this is the fastpath).  */
  if (atomic_bit_test_set (mutex, 31) == 0)
    return;

  atomic_increment (mutex);

  while (1)
    {
      if (atomic_bit_test_set (mutex, 31) == 0)
	{
	  atomic_decrement (mutex);
	  return;
	}

      /* We have to wait now. First make sure the futex value we are
	 monitoring is truly negative (i.e. locked). */
      v = *mutex;
      if (v >= 0)
	continue;

      lll_futex_wait (mutex, v,
		      // XYZ check mutex flag
		      LLL_SHARED);
    }
}


static inline void
__generic_mutex_unlock (int *mutex, int private)
{
  /* Adding 0x80000000 to the counter results in 0 if and only if
     there are not other interested threads - we can return (this is
     the fastpath).  */
  if (atomic_add_zero (mutex, 0x80000000))
    return;

  /* There are other threads waiting for this mutex, wake one of them
     up.  */
  lll_futex_wake (mutex, 1,
		  // XYZ check mutex flag
		  LLL_SHARED);
}


#define lll_lock(futex,private) __generic_mutex_lock (&(futex),private)
#define lll_unlock(futex,private) __generic_mutex_unlock (&(futex),private)


// needed to handle async communication - we assume this never happens ....
// TODO: verify if this breaks anything (hope not)
int __pthread_enable_asynccancel(){return 0;}
void __pthread_disable_asynccancel(int i){(void)i;}
#define __pthread_cleanup_push(x, y, z) {}
#define __pthread_cleanup_pop(x, y) {}


// pthreadP.h

/* Copyright (C) 2002-2007, 2009, 2011 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Ulrich Drepper <drepper@redhat.com>, 2002.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */



/* Atomic operations on TLS memory.  */
#ifndef THREAD_ATOMIC_CMPXCHG_VAL
# define THREAD_ATOMIC_CMPXCHG_VAL(descr, member, new, old) \
  atomic_compare_and_exchange_val_acq (&(descr)->member, new, old)
#endif

#ifndef THREAD_ATOMIC_BIT_SET
# define THREAD_ATOMIC_BIT_SET(descr, member, bit) \
  atomic_bit_set (&(descr)->member, bit)
#endif


/* Adaptive mutex definitions.  */
#ifndef MAX_ADAPTIVE_COUNT
# define MAX_ADAPTIVE_COUNT 100
#endif


/* Magic cookie representing robust mutex with dead owner.  */
#define PTHREAD_MUTEX_INCONSISTENT	INT_MAX
/* Magic cookie representing not recoverable robust mutex.  */
#define PTHREAD_MUTEX_NOTRECOVERABLE	(INT_MAX - 1)


/* Internal mutex type value.  */
enum
{
  PTHREAD_MUTEX_KIND_MASK_NP = 3,
  PTHREAD_MUTEX_ROBUST_NORMAL_NP = 16,
  PTHREAD_MUTEX_ROBUST_RECURSIVE_NP
  = PTHREAD_MUTEX_ROBUST_NORMAL_NP | PTHREAD_MUTEX_RECURSIVE_NP,
  PTHREAD_MUTEX_ROBUST_ERRORCHECK_NP
  = PTHREAD_MUTEX_ROBUST_NORMAL_NP | PTHREAD_MUTEX_ERRORCHECK_NP,
  PTHREAD_MUTEX_ROBUST_ADAPTIVE_NP
  = PTHREAD_MUTEX_ROBUST_NORMAL_NP | PTHREAD_MUTEX_ADAPTIVE_NP,
  PTHREAD_MUTEX_PRIO_INHERIT_NP = 32,
  PTHREAD_MUTEX_PI_NORMAL_NP
  = PTHREAD_MUTEX_PRIO_INHERIT_NP | PTHREAD_MUTEX_NORMAL,
  PTHREAD_MUTEX_PI_RECURSIVE_NP
  = PTHREAD_MUTEX_PRIO_INHERIT_NP | PTHREAD_MUTEX_RECURSIVE_NP,
  PTHREAD_MUTEX_PI_ERRORCHECK_NP
  = PTHREAD_MUTEX_PRIO_INHERIT_NP | PTHREAD_MUTEX_ERRORCHECK_NP,
  PTHREAD_MUTEX_PI_ADAPTIVE_NP
  = PTHREAD_MUTEX_PRIO_INHERIT_NP | PTHREAD_MUTEX_ADAPTIVE_NP,
  PTHREAD_MUTEX_PI_ROBUST_NORMAL_NP
  = PTHREAD_MUTEX_PRIO_INHERIT_NP | PTHREAD_MUTEX_ROBUST_NORMAL_NP,
  PTHREAD_MUTEX_PI_ROBUST_RECURSIVE_NP
  = PTHREAD_MUTEX_PRIO_INHERIT_NP | PTHREAD_MUTEX_ROBUST_RECURSIVE_NP,
  PTHREAD_MUTEX_PI_ROBUST_ERRORCHECK_NP
  = PTHREAD_MUTEX_PRIO_INHERIT_NP | PTHREAD_MUTEX_ROBUST_ERRORCHECK_NP,
  PTHREAD_MUTEX_PI_ROBUST_ADAPTIVE_NP
  = PTHREAD_MUTEX_PRIO_INHERIT_NP | PTHREAD_MUTEX_ROBUST_ADAPTIVE_NP,
  PTHREAD_MUTEX_PRIO_PROTECT_NP = 64,
  PTHREAD_MUTEX_PP_NORMAL_NP
  = PTHREAD_MUTEX_PRIO_PROTECT_NP | PTHREAD_MUTEX_NORMAL,
  PTHREAD_MUTEX_PP_RECURSIVE_NP
  = PTHREAD_MUTEX_PRIO_PROTECT_NP | PTHREAD_MUTEX_RECURSIVE_NP,
  PTHREAD_MUTEX_PP_ERRORCHECK_NP
  = PTHREAD_MUTEX_PRIO_PROTECT_NP | PTHREAD_MUTEX_ERRORCHECK_NP,
  PTHREAD_MUTEX_PP_ADAPTIVE_NP
  = PTHREAD_MUTEX_PRIO_PROTECT_NP | PTHREAD_MUTEX_ADAPTIVE_NP
};
#define PTHREAD_MUTEX_PSHARED_BIT 128

#define PTHREAD_MUTEX_TYPE(m) \
  ((m)->__data.__kind & 127)

#if LLL_PRIVATE == 0 && LLL_SHARED == 128
# define PTHREAD_MUTEX_PSHARED(m) \
  ((m)->__data.__kind & 128)
#else
# define PTHREAD_MUTEX_PSHARED(m) \
  (((m)->__data.__kind & 128) ? LLL_SHARED : LLL_PRIVATE)
#endif

/* The kernel when waking robust mutexes on exit never uses
   FUTEX_PRIVATE_FLAG FUTEX_WAKE.  */
#define PTHREAD_ROBUST_MUTEX_PSHARED(m) LLL_SHARED

/* Ceiling in __data.__lock.  __data.__lock is signed, so don't
   use the MSB bit in there, but in the mask also include that bit,
   so that the compiler can optimize & PTHREAD_MUTEX_PRIO_CEILING_MASK
   masking if the value is then shifted down by
   PTHREAD_MUTEX_PRIO_CEILING_SHIFT.  */
#define PTHREAD_MUTEX_PRIO_CEILING_SHIFT	19
#define PTHREAD_MUTEX_PRIO_CEILING_MASK		0xfff80000


/* Flags in mutex attr.  */
#define PTHREAD_MUTEXATTR_PROTOCOL_SHIFT	28
#define PTHREAD_MUTEXATTR_PROTOCOL_MASK		0x30000000
#define PTHREAD_MUTEXATTR_PRIO_CEILING_SHIFT	12
#define PTHREAD_MUTEXATTR_PRIO_CEILING_MASK	0x00fff000
#define PTHREAD_MUTEXATTR_FLAG_ROBUST		0x40000000
#define PTHREAD_MUTEXATTR_FLAG_PSHARED		0x80000000
#define PTHREAD_MUTEXATTR_FLAG_BITS \
  (PTHREAD_MUTEXATTR_FLAG_ROBUST | PTHREAD_MUTEXATTR_FLAG_PSHARED \
   | PTHREAD_MUTEXATTR_PROTOCOL_MASK | PTHREAD_MUTEXATTR_PRIO_CEILING_MASK)


/* Check whether rwlock prefers readers.   */
#define PTHREAD_RWLOCK_PREFER_READER_P(rwlock) \
  ((rwlock)->__data.__flags == 0)


/* Bits used in robust mutex implementation.  */
#define FUTEX_WAITERS		0x80000000
#define FUTEX_OWNER_DIED	0x40000000
#define FUTEX_TID_MASK		0x3fffffff





// pthread_cond_init.c

/* Copyright (C) 2002, 2003, 2004, 2005, 2007, 2008
   Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Ulrich Drepper <drepper@redhat.com>, 2002.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

int
__pthread_cond_init (cond, cond_attr)
     pthread_cond_t *cond;
     const pthread_condattr_t *cond_attr;
{
  struct pthread_condattr *icond_attr = (struct pthread_condattr *) cond_attr;

  cond->__data.__lock = LLL_LOCK_INITIALIZER;
  cond->__data.__futex = 0;
  cond->__data.__nwaiters = (icond_attr != NULL
			     ? ((icond_attr->value >> 1)
				& ((1 << COND_NWAITERS_SHIFT) - 1))
			     : CLOCK_REALTIME);
  cond->__data.__total_seq = 0;
  cond->__data.__wakeup_seq = 0;
  cond->__data.__woken_seq = 0;
  cond->__data.__mutex = (icond_attr == NULL || (icond_attr->value & 1) == 0
			  ? NULL : (void *) ~0l);
  cond->__data.__broadcast_seq = 0;

  return 0;
}


// pthread_cond_wait.c

/* Copyright (C) 2003,2004,2006,2007,2011 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Martin Schwidefsky <schwidefsky@de.ibm.com>, 2003.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */


struct _condvar_cleanup_buffer
{
  int oldtype;
  pthread_cond_t *cond;
  pthread_mutex_t *mutex;
  unsigned int bc_seq;
};


void
__attribute__ ((visibility ("hidden")))
__condvar_cleanup (void *arg)
{
  struct _condvar_cleanup_buffer *cbuffer =
    (struct _condvar_cleanup_buffer *) arg;
  unsigned int destroying;
  int pshared = (cbuffer->cond->__data.__mutex == (void *) ~0l)
  		? LLL_SHARED : LLL_PRIVATE;

  /* We are going to modify shared data.  */
  lll_lock (cbuffer->cond->__data.__lock, pshared);

  if (cbuffer->bc_seq == cbuffer->cond->__data.__broadcast_seq)
    {
      /* This thread is not waiting anymore.  Adjust the sequence counters
	 appropriately.  We do not increment WAKEUP_SEQ if this would
	 bump it over the value of TOTAL_SEQ.  This can happen if a thread
	 was woken and then canceled.  */
      if (cbuffer->cond->__data.__wakeup_seq
	  < cbuffer->cond->__data.__total_seq)
	{
	  ++cbuffer->cond->__data.__wakeup_seq;
	  ++cbuffer->cond->__data.__futex;
	}
      ++cbuffer->cond->__data.__woken_seq;
    }

  cbuffer->cond->__data.__nwaiters -= 1 << COND_NWAITERS_SHIFT;

  /* If pthread_cond_destroy was called on this variable already,
     notify the pthread_cond_destroy caller all waiters have left
     and it can be successfully destroyed.  */
  destroying = 0;
  if (cbuffer->cond->__data.__total_seq == -1ULL
      && cbuffer->cond->__data.__nwaiters < (1 << COND_NWAITERS_SHIFT))
    {
      lll_futex_wake (&cbuffer->cond->__data.__nwaiters, 1, pshared);
      destroying = 1;
    }

  /* We are done.  */
  lll_unlock (cbuffer->cond->__data.__lock, pshared);

  /* Wake everybody to make sure no condvar signal gets lost.  */
  if (! destroying)
    lll_futex_wake (&cbuffer->cond->__data.__futex, INT_MAX, pshared);

  /* Get the mutex before returning unless asynchronous cancellation
     is in effect.  */
  __pthread_mutex_cond_lock (cbuffer->mutex);
}


int
__pthread_cond_wait (cond, mutex)
     pthread_cond_t *cond;
     pthread_mutex_t *mutex;
{
  struct _pthread_cleanup_buffer buffer;
  struct _condvar_cleanup_buffer cbuffer;
  int err;
  int pshared = (cond->__data.__mutex == (void *) ~0l)
  		? LLL_SHARED : LLL_PRIVATE;

  /* Make sure we are alone.  */
  lll_lock (cond->__data.__lock, pshared);

  /* Now we can release the mutex.  */
  err = __pthread_mutex_unlock_usercnt (mutex, 0);
  if (__builtin_expect (err, 0))
    {
      lll_unlock (cond->__data.__lock, pshared);
      return err;
    }

  /* We have one new user of the condvar.  */
  ++cond->__data.__total_seq;
  ++cond->__data.__futex;
  cond->__data.__nwaiters += 1 << COND_NWAITERS_SHIFT;

  /* Remember the mutex we are using here.  If there is already a
     different address store this is a bad user bug.  Do not store
     anything for pshared condvars.  */
  if (cond->__data.__mutex != (void *) ~0l)
    cond->__data.__mutex = mutex;

  /* Prepare structure passed to cancellation handler.  */
  cbuffer.cond = cond;
  cbuffer.mutex = mutex;

  /* Before we block we enable cancellation.  Therefore we have to
     install a cancellation handler.  */
  __pthread_cleanup_push (&buffer, __condvar_cleanup, &cbuffer);

  /* The current values of the wakeup counter.  The "woken" counter
     must exceed this value.  */
  unsigned long long int val;
  unsigned long long int seq;
  val = seq = cond->__data.__wakeup_seq;
  /* Remember the broadcast counter.  */
  cbuffer.bc_seq = cond->__data.__broadcast_seq;

  int tries = 0;
  do
    {
      unsigned int futex_val = cond->__data.__futex;

      /* Prepare to wait.  Release the condvar futex.  */
      lll_unlock (cond->__data.__lock, pshared);

      /* Enable asynchronous cancellation.  Required by the standard.  */
      cbuffer.oldtype = __pthread_enable_asynccancel ();

      // speculate if futex is held
      if(TM_COND_VARS && (tries < TK_NUM_TRIES) && (cond->__data.__futex == futex_val)){
          if(enter_htm(cond)==0){return 0;}
          else{
            tries++;
          }
      }
      
      
      /* Wait until woken by signal or broadcast.  */
      lll_futex_wait (&cond->__data.__futex, futex_val, pshared);

      /* Disable asynchronous cancellation.  */
      __pthread_disable_asynccancel (cbuffer.oldtype);

      /* We are going to look at shared data again, so get the lock.  */
      lll_lock (cond->__data.__lock, pshared);

      /* If a broadcast happened, we are done.  */
      if (cbuffer.bc_seq != cond->__data.__broadcast_seq)
	goto bc_out;

      /* Check whether we are eligible for wakeup.  */
      val = cond->__data.__wakeup_seq;
    }
  while (val == seq || cond->__data.__woken_seq == val);

  /* Another thread woken up.  */
  ++cond->__data.__woken_seq;

 bc_out:

  cond->__data.__nwaiters -= 1 << COND_NWAITERS_SHIFT;

  /* If pthread_cond_destroy was called on this varaible already,
     notify the pthread_cond_destroy caller all waiters have left
     and it can be successfully destroyed.  */
  if (cond->__data.__total_seq == -1ULL
      && cond->__data.__nwaiters < (1 << COND_NWAITERS_SHIFT))
    lll_futex_wake (&cond->__data.__nwaiters, 1, pshared);

  /* We are done with the condvar.  */
  lll_unlock (cond->__data.__lock, pshared);

  /* The cancellation handling is back to normal, remove the handler.  */
  __pthread_cleanup_pop (&buffer, 0);

  /* Get the mutex before returning.  */
  return __pthread_mutex_cond_lock (mutex);
}



// pthread_cond_signal.c


/* Copyright (C) 2003, 2004, 2007 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Martin Schwidefsky <schwidefsky@de.ibm.com>, 2003.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */


int
__pthread_cond_signal (cond)
     pthread_cond_t *cond;
{
  int pshared = (cond->__data.__mutex == (void *) ~0l)
		? LLL_SHARED : LLL_PRIVATE;

  /* Make sure we are alone.  */
  lll_lock (cond->__data.__lock, pshared);

  /* Are there any waiters to be woken?  */
  if (cond->__data.__total_seq > cond->__data.__wakeup_seq)
    {
      /* Yes.  Mark one of them as woken.  */
      ++cond->__data.__wakeup_seq;
      ++cond->__data.__futex;

      /* Wake one.  */
      if (! __builtin_expect (lll_futex_wake_unlock (&cond->__data.__futex, 1,
						     1, &cond->__data.__lock,
						     pshared), 0))
	return 0;

      lll_futex_wake (&cond->__data.__futex, 1, pshared);
    }

  /* We are done.  */
  lll_unlock (cond->__data.__lock, pshared);

  return 0;
}




// pthread_broadcast


/* Copyright (C) 2003, 2004, 2006, 2007 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Martin Schwidefsky <schwidefsky@de.ibm.com>, 2003.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

int
__pthread_cond_broadcast (cond)
     pthread_cond_t *cond;
{
  int pshared = (cond->__data.__mutex == (void *) ~0l)
		? LLL_SHARED : LLL_PRIVATE;
  /* Make sure we are alone.  */
  lll_lock (cond->__data.__lock, pshared);

  /* Are there any waiters to be woken?  */
  if (cond->__data.__total_seq > cond->__data.__wakeup_seq)
    {
      /* Yes.  Mark them all as woken.  */
      cond->__data.__wakeup_seq = cond->__data.__total_seq;
      cond->__data.__woken_seq = cond->__data.__total_seq;
      cond->__data.__futex = (unsigned int) cond->__data.__total_seq * 2;
      int futex_val = cond->__data.__futex;
      /* Signal that a broadcast happened.  */
      ++cond->__data.__broadcast_seq;

      /* We are done.  */
      lll_unlock (cond->__data.__lock, pshared);

      /* Do not use requeue for pshared condvars.  */
      if (cond->__data.__mutex == (void *) ~0l)
	goto wake_all;


      /* Wake everybody.  */
      pthread_mutex_t *mut = (pthread_mutex_t *) cond->__data.__mutex;

      /* XXX: Kernel so far doesn't support requeue to PI futex.  */
      /* XXX: Kernel so far can only requeue to the same type of futex,
	 in this case private (we don't requeue for pshared condvars).  */
      if (__builtin_expect (mut->__data.__kind
			    & (PTHREAD_MUTEX_PRIO_INHERIT_NP
			       | PTHREAD_MUTEX_PSHARED_BIT), 0))
	goto wake_all;


    goto wake_all; // JI- skip all the optimizations below because I'm pretty sure they're buggy



      /* lll_futex_requeue returns 0 for success and non-zero
	 for errors.  */
      if (__builtin_expect (lll_futex_requeue (&cond->__data.__futex, 1,
					       INT_MAX, &mut->__data.__lock,
					       futex_val, LLL_PRIVATE), 0))
	{
	  /* The requeue functionality is not available.  */
	wake_all:
	  lll_futex_wake (&cond->__data.__futex, INT_MAX, pshared);
	}

      /* That's all.  */
      return 0;
    }

  /* We are done.  */
  lll_unlock (cond->__data.__lock, pshared);

  return 0;
}


// pthread_cond_timedwait.c

/* Copyright (C) 2003,2004,2007,2010,2011 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Martin Schwidefsky <schwidefsky@de.ibm.com>, 2003.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

#ifndef HAVE_CLOCK_GETTIME_VSYSCALL
# undef INTERNAL_VSYSCALL
# define INTERNAL_VSYSCALL INTERNAL_SYSCALL
# undef INLINE_VSYSCALL
# define INLINE_VSYSCALL INLINE_SYSCALL
#else
# include <bits/libc-vdso.h>
#endif
   
   
int
__pthread_cond_timedwait (cond, mutex, abstime)
     pthread_cond_t *cond;
     pthread_mutex_t *mutex;
     const struct timespec *abstime;
{
  struct _pthread_cleanup_buffer buffer;
  struct _condvar_cleanup_buffer cbuffer;
  int result = 0;

  /* Catch invalid parameters.  */
  if (abstime->tv_nsec < 0 || abstime->tv_nsec >= 1000000000)
    return EINVAL;

  int pshared = (cond->__data.__mutex == (void *) ~0l)
		? LLL_SHARED : LLL_PRIVATE;

  /* Make sure we are alone.  */
  lll_lock (cond->__data.__lock, pshared);

  /* Now we can release the mutex.  */
  int err = __pthread_mutex_unlock_usercnt (mutex, 0);
  if (err)
    {
      lll_unlock (cond->__data.__lock, pshared);
      return err;
    }

  /* We have one new user of the condvar.  */
  ++cond->__data.__total_seq;
  ++cond->__data.__futex;
  cond->__data.__nwaiters += 1 << COND_NWAITERS_SHIFT;

  /* Remember the mutex we are using here.  If there is already a
     different address store this is a bad user bug.  Do not store
     anything for pshared condvars.  */
  if (cond->__data.__mutex != (void *) ~0l)
    cond->__data.__mutex = mutex;

  /* Prepare structure passed to cancellation handler.  */
  cbuffer.cond = cond;
  cbuffer.mutex = mutex;

  /* Before we block we enable cancellation.  Therefore we have to
     install a cancellation handler.  */
  __pthread_cleanup_push (&buffer, __condvar_cleanup, &cbuffer);

  /* The current values of the wakeup counter.  The "woken" counter
     must exceed this value.  */
  unsigned long long int val;
  unsigned long long int seq;
  val = seq = cond->__data.__wakeup_seq;
  /* Remember the broadcast counter.  */
  cbuffer.bc_seq = cond->__data.__broadcast_seq;

  while (1)
    {
      struct timespec rt;
      {
//#ifdef __NR_clock_gettime
#if FALSE
	INTERNAL_SYSCALL_DECL (err);
	int ret;
	ret = INTERNAL_VSYSCALL (clock_gettime, err, 2,
				(cond->__data.__nwaiters
				 & ((1 << COND_NWAITERS_SHIFT) - 1)),
				&rt);
# ifndef __ASSUME_POSIX_TIMERS
	if (__builtin_expect (INTERNAL_SYSCALL_ERROR_P (ret, err), 0))
	  {
	    struct timeval tv;
	    (void) gettimeofday (&tv, NULL);

	    /* Convert the absolute timeout value to a relative timeout.  */
	    rt.tv_sec = abstime->tv_sec - tv.tv_sec;
	    rt.tv_nsec = abstime->tv_nsec - tv.tv_usec * 1000;
	  }
	else
# endif
	  {
	    /* Convert the absolute timeout value to a relative timeout.  */
	    rt.tv_sec = abstime->tv_sec - rt.tv_sec;
	    rt.tv_nsec = abstime->tv_nsec - rt.tv_nsec;
	  }
#else
	/* Get the current time.  So far we support only one clock.  */
	struct timeval tv;
	(void) gettimeofday (&tv, NULL);

	/* Convert the absolute timeout value to a relative timeout.  */
	rt.tv_sec = abstime->tv_sec - tv.tv_sec;
	rt.tv_nsec = abstime->tv_nsec - tv.tv_usec * 1000;
#endif
      }
      if (rt.tv_nsec < 0)
	{
	  rt.tv_nsec += 1000000000;
	  --rt.tv_sec;
	}
      /* Did we already time out?  */
      if (__builtin_expect (rt.tv_sec < 0, 0))
	{
	  if (cbuffer.bc_seq != cond->__data.__broadcast_seq)
	    goto bc_out;

	  goto timeout;
	}

      unsigned int futex_val = cond->__data.__futex;

      /* Prepare to wait.  Release the condvar futex.  */
      lll_unlock (cond->__data.__lock, pshared);

      /* Enable asynchronous cancellation.  Required by the standard.  */
      cbuffer.oldtype = __pthread_enable_asynccancel ();

      /* Wait until woken by signal or broadcast.  */
      err = lll_futex_timed_wait (&cond->__data.__futex,
				  futex_val, &rt, pshared);

      /* Disable asynchronous cancellation.  */
      __pthread_disable_asynccancel (cbuffer.oldtype);

      /* We are going to look at shared data again, so get the lock.  */
      lll_lock (cond->__data.__lock, pshared);

      /* If a broadcast happened, we are done.  */
      if (cbuffer.bc_seq != cond->__data.__broadcast_seq)
	goto bc_out;

      /* Check whether we are eligible for wakeup.  */
      val = cond->__data.__wakeup_seq;
      if (val != seq && cond->__data.__woken_seq != val)
	break;

      /* Not woken yet.  Maybe the time expired?  */
      if (__builtin_expect (err == -ETIMEDOUT, 0))
	{
	timeout:
	  /* Yep.  Adjust the counters.  */
	  ++cond->__data.__wakeup_seq;
	  ++cond->__data.__futex;

	  /* The error value.  */
	  result = ETIMEDOUT;
	  break;
	}
    }

  /* Another thread woken up.  */
  ++cond->__data.__woken_seq;

 bc_out:

  cond->__data.__nwaiters -= 1 << COND_NWAITERS_SHIFT;

  /* If pthread_cond_destroy was called on this variable already,
     notify the pthread_cond_destroy caller all waiters have left
     and it can be successfully destroyed.  */
  if (cond->__data.__total_seq == -1ULL
      && cond->__data.__nwaiters < (1 << COND_NWAITERS_SHIFT))
    lll_futex_wake (&cond->__data.__nwaiters, 1, pshared);

  /* We are done with the condvar.  */
  lll_unlock (cond->__data.__lock, pshared);

  /* The cancellation handling is back to normal, remove the handler.  */
  __pthread_cleanup_pop (&buffer, 0);

  /* Get the mutex before returning.  */
  err = __pthread_mutex_cond_lock (mutex);

  return err ?: result;
}

int pthread_cond_destroy(pthread_cond_t *cond){return 0;}

#endif