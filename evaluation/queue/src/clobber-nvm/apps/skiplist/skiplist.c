/*
 * Volatile skiplist originally implemented as https://github.com/begeekmyfriend/skiplist/blob/master/skiplist.h
 * See LICENSE for more information
 */

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include "skiplist.h"
#include <string.h>

uint64_t hash(char *str) {
    uint64_t hash = 5381;
    int c;
    while (*str != '\0') {
        c = *str;
        hash = ((hash << 5) + hash) + c;
        str++;
    }
    return hash;
}

void listDestroy(struct skiplist **list){
	skiplist_delete(*list);
}

void listCreate(struct skiplist **list){
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&lock);

    *list = skiplist_new();
    pthread_mutex_unlock(&lock);
}

void doInsert(struct skiplist *list, char* key, size_t ksize, char* value, size_t vsize){
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&lock);

    uint64_t key_int = hash(key);
    skiplist_insert(list, key_int, value);

    pthread_mutex_unlock(&lock);
}


void doRead(struct skiplist *list, char* key, char* value){
    uint64_t key_int = hash(key);
    struct skipnode *node = skiplist_search(list, key_int);
    if(node!=NULL){
	pthread_rwlock_rdlock(&(node->lock));
	memcpy(value, node->value, strlen(node->value));
	pthread_rwlock_unlock(&(node->lock));
    }
}


void doUpdate(struct skiplist *list, char* key, size_t ksize, char* value, size_t vsize){
    uint64_t key_int = hash(key);
    struct skipnode *node = skiplist_search(list, key_int);
    if(node!=NULL){
	pthread_rwlock_wrlock(&(node->lock));
        memcpy(node->value, value, strlen(value));
	pthread_rwlock_unlock(&(node->lock));
    }
}

void list_init(struct sk_link *link)
{
    link->prev = link;
    link->next = link;
}

void
__list_add(struct sk_link *link, struct sk_link *prev, struct sk_link *next)
{
    link->next = next;
    link->prev = prev;
    next->prev = link;
    prev->next = link;
}

void __list_del(struct sk_link *prev, struct sk_link *next)
{
    prev->next = next;
    next->prev = prev;
}

void list_add(struct sk_link *link, struct sk_link *prev)
{
    __list_add(link, prev, prev->next);
}

void list_del(struct sk_link *link)
{
    __list_del(link->prev, link->next);
    list_init(link);
}

int list_empty(struct sk_link *link)
{
    return link->next == link;
}


struct skipnode *skipnode_new(struct skiplist *list, int level, uint64_t key, char* value)
{
    struct skipnode *node;
    node = malloc(sizeof(*node) + level * sizeof(struct sk_link));
    if (node != NULL) {
	node->key = key;
	node->value = list->value_addr + VALUE_SIZE * list->count;
	memcpy(node->value, value, strlen(value));
	pthread_rwlock_init(&(node->lock), NULL);
    }
    return node;
}

void skipnode_delete(struct skipnode *node)
{
    free(node->value);
    free(node);
}

struct skiplist *skiplist_new(void)
{
    int i;
    struct skiplist *list = malloc(sizeof(*list));
    if (list != NULL) {
	list->level = 1;
	list->count = 0;
	for (i = 0; i < sizeof(list->head) / sizeof(list->head[0]); i++) {
	    list_init(&list->head[i]);
	    pthread_rwlock_init(&(list->locks[i]), NULL);
	}
	pthread_rwlock_init(&(list->lock), NULL);
    }

    list->value_addr = malloc(VALUE_SIZE * VALUE_COUNT);
    return list;
}

void skiplist_delete(struct skiplist *list)
{
    struct sk_link *n;
    struct sk_link *pos = list->head[0].next;
    skiplist_foreach_safe(pos, n, &list->head[0]) {
	struct skipnode *node = list_entry(pos, struct skipnode, link[0]);
	skipnode_delete(node);
    }
    free(list);
}

int random_level(void)
{
    int level = 1;
    const double p = 0.25;
    while ((random() & 0xffff) < 0xffff * p) {
	level++;
    }
    return level > MAX_LEVEL ? MAX_LEVEL : level;
}

struct skipnode *skiplist_search(struct skiplist *list, uint64_t key)
{
    struct skipnode *node;
    int i = list->level - 1;
    struct sk_link *pos = &list->head[i];
    struct sk_link *end = &list->head[i];

    for (; i >= 0; i--) {
	pos = pos->next;
	skiplist_foreach(pos, end) {
	    node = list_entry(pos, struct skipnode, link[i]);
	    if (node->key >= key) {
		end = &node->link[i];
		break;
	    }
	}
	if (node->key == key) {
	    return node;
	}
	pos = end->prev;
	pos--;
	end--;
    }
    return NULL;
}


struct skipnode *
skiplist_insert(struct skiplist *list, uint64_t key, char* value)
{
    int level = random_level();
    if (level > list->level) {
	list->level = level;
    }

    pthread_rwlock_wrlock(&(list->lock));
    struct skipnode *node = skipnode_new(list, level, key, value);
		
    if (node != NULL) {
	int i = list->level - 1;

	struct sk_link *pos = &list->head[i];
	struct sk_link *end = &list->head[i];

	for (; i >= 0; i--) {
	    pos = pos->next;
	    skiplist_foreach(pos, end) {
		struct skipnode *nd = list_entry(pos, struct skipnode, link[i]);
		if (nd->key >= key) {
		    end = &nd->link[i];
		    break;
		}
	    }
	    pos = end->prev;
	    if (i < level) {
		__list_add(&node->link[i], pos, end);
	    }
	    pos--;
	    end--;
	}
	list->count++;
    }
    pthread_rwlock_unlock(&(list->lock));
    return node;
}

void __remove(struct skiplist *list, struct skipnode *node, int level)
{
    int i;
    for (i = 0; i < level; i++) {
	list_del(&node->link[i]);
	if (list_empty(&list->head[i])) {
	    list->level--;
	}
    }
    skipnode_delete(node);
    list->count--;
}

void skiplist_remove(struct skiplist *list, uint64_t key)
{
    struct sk_link *n;
    struct skipnode *node;
    int i = list->level - 1;
    struct sk_link *pos = &list->head[i];
    struct sk_link *end = &list->head[i];

    for (; i >= 0; i--) {
	pos = pos->next;
	skiplist_foreach_safe(pos, n, end) {
	node = list_entry(pos, struct skipnode, link[i]);
	    if (node->key > key) {
		end = &node->link[i];
		break;
	    } else if (node->key == key) {
		/* we allow nodes with same key. */
		__remove(list, node, i + 1);
	    }
	}
	pos = end->prev;
	pos--;
	end--;
    }
}

