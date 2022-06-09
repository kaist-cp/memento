/***************** context.h *********************/
#include <libpmemobj.h>

#include <stdio.h>
#include <inttypes.h>

#define PMemPath            "/mnt/pmem0/memcached.pop"
#define PMemSize            ((size_t) 8 << 30)
#define PMemBoundary        0x1000000000000
#define IS_NVMM(ptr)        ((uint64_t)ptr & PMemBoundary)
#define ABS_PTR(type, ptr)  (type *)((uintptr_t)basePtr + ((uint64_t)ptr & (PMemBoundary - 1)))
#define MaxThreads          128
#define funcPtrSize	    2048

#ifdef DEBUG
#define debug(fmt, ...) fprintf(stdout, fmt, __VA_ARGS__)
#else
#define debug(fmt, ...) {}
#endif

typedef struct {
    uint64_t id;
    uint64_t index;
    int32_t locksHeld;
    uint32_t bytesAllocated;
    uint64_t openTxs;
    uint64_t funcPtrOffset; // offset from pop_base of the address that function pointers store at
    uint64_t funcPtr;
    uint64_t v_Buffer;//volatile buffer for coelease arguments

    // debug statistics
    uint64_t bytesWritten; // on_nvmm_write()
    uint64_t mallocs; // persistent allocations
    uint64_t frees; // persistent frees

    uint64_t reserved[2];
} ThreadContext;

ThreadContext *my_context();

void *init_runtime();
void finalize_runtime();
void tx_open(ThreadContext *);
void tx_commit(ThreadContext *);
void *pmem_alloc(size_t);
void *pmem_tx_alloc(size_t);
void pmem_free(void *);

void *pmalloc(size_t size);
void pfree(void *ptr);

int tx_lock();
int tx_unlock();


size_t nvmm_strlen(void *ptr);


/********************** end of context.h ****************************/

/********************** admin_pop.h *********************************/

#define LAYOUT "linkedlist"
#define POOL_SIZE 1024*1024*1024
#define admin_path "/mnt/pmem0/adminpool.pop"
//#define PMemPath            "/mnt/pmem0/pool2.pop"

POBJ_LAYOUT_BEGIN(linkedlist);
POBJ_LAYOUT_ROOT(linkedlist, struct list_head);
POBJ_LAYOUT_TOID(linkedlist, struct list_elem);
POBJ_LAYOUT_END(linkedlist);

static PMEMobjpool* admin_pop = NULL;
static TOID(struct list_head) root;

struct list_head {
        PMEMmutex lock;
        int num_elements;
        TOID(struct list_elem) head;
        TOID(struct list_elem) tail;
};

struct list_elem {
        uint64_t funcptr_offset;
	//TODO: func name ptr
        TOID(struct list_elem) next;
};



void* init_admin_pop();
void add_node(uint64_t offset);
int get_all_elem(uint64_t* offset);
void scan_all_offset();
void admin_pop_close();
int admin_pop_check();
void* get_baseptr();

/**************** end of admin_pop.h ***********************/
