#ifndef _SKIPLIST_H
#define _SKIPLIST_H

#include <stdint.h>
#include <pthread.h>

#define list_entry(ptr, type, member) \
        ((type *)((char *)(ptr) - (size_t)(&((type *)0)->member)))

#define skiplist_foreach(pos, end) \
        for (; pos != end; pos = pos->next)

#define skiplist_foreach_safe(pos, n, end) \
        for (n = pos->next; pos != end; pos = n, n = pos->next)

#define MAX_LEVEL 256  /* Should be enough for 2^32 elements */

#define VALUE_SIZE 512
#define VALUE_COUNT 1000000

struct sk_link {
        struct sk_link *prev, *next;
};


typedef struct skiplist {
		pthread_rwlock_t lock;
        int level;
        int count;
	void* value_addr;
		pthread_rwlock_t locks[MAX_LEVEL];
        struct sk_link head[MAX_LEVEL];
}skiplist;

struct skipnode {
		pthread_rwlock_t lock;
        uint64_t key;
        char* value;
        struct sk_link link[0];
};

uint64_t hash(char *str);

void list_init(struct sk_link *link);

void
__list_add(struct sk_link *link, struct sk_link *prev, struct sk_link *next);

void __list_del(struct sk_link *prev, struct sk_link *next);

void list_add(struct sk_link *link, struct sk_link *prev);

void list_del(struct sk_link *link);

int list_empty(struct sk_link *link);

struct skipnode *skipnode_new(struct skiplist *list, int level, uint64_t key, char* value);

void skipnode_delete(struct skipnode *node);

struct skiplist *skiplist_new(void);

void skiplist_delete(struct skiplist *list);

int random_level(void);

struct skipnode *skiplist_search(struct skiplist *list, uint64_t key);

struct skipnode *
skiplist_insert(struct skiplist *list, uint64_t key, char* value);

void __remove(struct skiplist *list, struct skipnode *node, int level);

void skiplist_remove(struct skiplist *list, uint64_t key);

void skiplist_dump(struct skiplist *list);


void doInsert(struct skiplist *list, char* key, size_t ksize, char* value, size_t vsize);
void doRead(struct skiplist *list, char* key, char* value);
void doUpdate(struct skiplist *list, char* key, size_t ksize, char* value, size_t vsize);

void listCreate(struct skiplist **list);
void listDestroy(struct skiplist **list);
#endif  /* _SKIPLIST_H */


