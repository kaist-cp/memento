
#ifndef UTILITIES_H_
#define UTILITIES_H_

#include <climits>				//for max int
#include <fstream>
#include <iostream>
#include <xmmintrin.h>

#include <numa.h>

#ifdef PMEM
#include <libpmem.h>
#endif

using namespace std;

#define MAX_THREADS 96         // upper bound on the number of threads in your system
#define PADDING 512            // Padding must be multiple of 4 for proper alignment
#define CAS __sync_bool_compare_and_swap
#define MFENCE __sync_synchronize

// --------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------

#define CACHE_LINE_SIZE      128
#define PAD_CACHE(A)         ((CACHE_LINE_SIZE - (A % CACHE_LINE_SIZE))/sizeof(char))
#define CACHE_ALIGN          alignas(CACHE_LINE_SIZE)
#define BOT                  (1)
#define CPU_CORES	         sysconf(_SC_NPROCESSORS_ONLN)

#ifndef MAX_WORK
#define MAX_WORK             512
#endif

#define SIM_RAND_MAX         32768

static uint32_t __ncores = 0;
static __thread int32_t __prefered_core = -1;

inline uint32_t getNCores(void) {
    if (__ncores == 0)
        __ncores = sysconf(_SC_NPROCESSORS_ONLN);
    return __ncores;
}

inline uint32_t preferedCoreOfThread(uint32_t pid) {
    uint32_t prefered_core = 0;
    int ncpus = numa_num_configured_cpus();
    int nodes = numa_num_task_nodes();
    int node_size = ncpus / nodes;

    if (numa_node_of_cpu(0) == numa_node_of_cpu(ncpus / 2)) {
        int half_node_size = node_size / 2;
        int offset = 0;
        uint32_t half_cpu_id = pid;

        if (pid >= ncpus / 2) {
            half_cpu_id = pid - ncpus / 2;
            offset = ncpus / 2;
        }
        prefered_core = (half_cpu_id % nodes) * half_node_size + half_cpu_id / nodes;
        prefered_core += offset;
    } else {
        prefered_core = ((pid % nodes) * node_size);
    }

    prefered_core %= getNCores();

    return prefered_core;
}


int threadPin(int32_t cpu_id) {
    int ret = 0;
    cpu_set_t mask;
    unsigned int len = sizeof(mask);

    pthread_setconcurrency(getNCores());
    CPU_ZERO(&mask);
    __prefered_core = preferedCoreOfThread(cpu_id);
    CPU_SET(__prefered_core, &mask);
#if defined(DEBUG)
    fprintf(stderr, "DEBUG: thread: %d -- numa_node: %d -- core: %d\n", cpu_id, numa_node_of_cpu(__prefered_core), __prefered_core);
#endif
    ret = sched_setaffinity(0, len, &mask);
    if (ret == -1)
        perror("sched_setaffinity");

    return ret;
}

// --------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------

static thread_local long __fast_random_next = 1;
static thread_local uint32_t __fast_random_next_z = 2;
static thread_local uint32_t __fast_random_next_w = 2;

long fastRandom(void) {
    __fast_random_next = __fast_random_next * 1103515245 + 12345;
    return((unsigned)(__fast_random_next/65536) % 32768);
}

// A simple pseudo-random 32-bit number generator implementing the multiply-with-carry method 
// invented by George Marsaglia. It is computationally fast and has good properties.
// http://en.wikipedia.org/wiki/Random_number_generation#Computational_methods
uint32_t fastRandom32(void) {
    __fast_random_next_z = 36969 * (__fast_random_next_z & 65535) + (__fast_random_next_z >> 16);
    __fast_random_next_w = 18000 * (__fast_random_next_w & 65535) + (__fast_random_next_w >> 16);
    return (__fast_random_next_z << 16) + __fast_random_next_w;  /* 32-bit result */
}

void fastRandomSetSeed(uint32_t seed) {
    __fast_random_next = (long)seed;
    __fast_random_next_z = seed;
    __fast_random_next_w = seed/2;

    if (__fast_random_next_z == 0 || __fast_random_next_z == 0x9068ffff)
        __fast_random_next_z++;
    if (__fast_random_next_w == 0 || __fast_random_next_w == 0x464fffff)
        __fast_random_next_w++;
}

// In Numerical Recipes in C: The Art of Scientific Computing 
// (William H. Press, Brian P. Flannery, Saul A. Teukolsky, William T. Vetterling;
// New York: Cambridge University Press, 1992 (2nd ed., p. 277))
// -------------------------------------------------------------------------------
uint32_t fastRandomRange32(uint32_t low, uint32_t high) {
    return low + (uint32_t) ( ((double) high)* ((double)fastRandom32() / (UINT_MAX)));
}

long fastRandomRange(long low, long high) {
    return low + (long) (((double) high)* ((double)fastRandom() / (SIM_RAND_MAX + 1.0)));
}

void randomWork() {
    volatile long j;
    long rnum;
    
    rnum = fastRandomRange(1, MAX_WORK);
    for (j = 0; j < rnum; j++)
        ;
}

void SFENCE()
{
    asm volatile ("sfence" ::: "memory");
}

void NOOP()
{

}

#ifndef PSYNC_OFF
#ifdef PWB_IS_CLFLUSH
    #define PFENCE      NOOP
    #define PSYNC       NOOP
    #define PDETECT     MFENCE
#elif PWB_IS_CLFLUSHOPT
    #define PFENCE      SFENCE
    #define PSYNC       SFENCE
    #define PDETECT     SFENCE
#elif PWB_IS_CLWB
    #define PFENCE      pmem_drain
    #define PSYNC       pmem_drain
    #define PDETECT     pmem_drain
#else
#error "You must define what PWB is. Choose PWB_IS_CLFLUSH if you don't know what your CPU is capable of"
#endif
#else
    #define PFENCE      NOOP
    #define PSYNC       NOOP
    #define PDETECT     NOOP
#endif

// --------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------

std::ofstream file;

void PWB(void *p)
{
#ifdef PWB_IS_CLFLUSH
    asm volatile ("clflush (%0)" :: "r"(p));
#elif PWB_IS_CLFLUSHOPT
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(p)));    // clflushopt (Kaby Lake)
#elif PWB_IS_CLWB
    pmem_flush(p, sizeof(p));
    // asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(p)));  // clwb() only for Ice Lake onwards
#else
#error "You must define what PWB is. Choose PWB_IS_CLFLUSH if you don't know what your CPU is capable of"
#endif
}

void PWB(void volatile * p)
{
#ifdef PWB_IS_CLFLUSH
    asm volatile ("clflush (%0)" :: "r"(p));
#elif PWB_IS_CLFLUSHOPT
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(p)));    // clflushopt (Kaby Lake)
#elif PWB_IS_CLWB
    pmem_flush((const void *)p, sizeof(p));
    // asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(p)));  // clwb() only for Ice Lake onwards
#else
#error "You must define what PWB is. Choose PWB_IS_CLFLUSH if you don't know what your CPU is capable of"
#endif
}

#ifdef LOW_PWBS
    #define PWB_LOW(x) PWB(x)
#else
    #define PWB_LOW(x)
#endif
#ifdef MED_PWBS
    #define PWB_MED(x) PWB(x)
#else
    #define PWB_MED(x)
#endif
#ifdef HIGH_PWBS
    #define PWB_HIGH(x) PWB(x)
#else
    #define PWB_HIGH(x)
#endif


#define BARRIER(p) {PWB(p);PFENCE();}
#define OPT_BARRIER(p) {PWB(p);}

#ifdef MANUAL_FLUSH
	#define MANUAL(x) x
#else
    #define MANUAL(x)               
#endif

#ifdef READ_WRITE_FLUSH
	#define RFLUSH(x) x
	#define WFLUSH(x) x
#else
	#ifdef WRITE_FLUSH
		#define RFLUSH(x) {}
		#define WFLUSH(x) x
	#else
		#define RFLUSH(x) {}
		#define WFLUSH(x) {}
	#endif
#endif


#endif /* UTILITIES_H_ */
