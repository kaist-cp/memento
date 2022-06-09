#include <libpmemobj.h>

#define LAYOUT "linkedlist"
#define POOL_SIZE 1024*1024*1024
#define admin_path "/mnt/pmem0/admin_pool.pop"
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
