#ifndef __RBTREE_H__
#define __RBTREE_H__
#include <stdlib.h>
#include <stdint.h>

typedef unsigned long long ULL;

enum rb_color
{
    RB_BLACK,
    RB_RED,
};

typedef struct rbtree_node
{
    struct rbtree_node* parent;
    struct rbtree_node* left;
    struct rbtree_node* right;
    enum rb_color color;
    uint64_t key;
    void *data;
	pthread_rwlock_t lock;
}rbtree_node;

typedef int (*rbtree_cmp_fn_t)(uint64_t key_a,  uint64_t key_b);
typedef struct rbtree
{
	pthread_rwlock_t lock;
    struct rbtree_node* root;
    rbtree_cmp_fn_t compare; 
}rbtree;

struct rbtree* rbtree_init(rbtree_cmp_fn_t fn);
int  rbtree_insert(struct rbtree *tree, uint64_t key, void* data);
void*  rbtree_lookup(struct rbtree* tree, uint64_t key);
int  rbtree_remove(struct rbtree* tree, uint64_t key);

void rbtreeCreate(rbtree **tree);
void rbtreeDestroy(rbtree **tree);
void doInsert(rbtree *tree, char* key, size_t ks, char* value, size_t vs);
void doUpdate(rbtree *tree, char* key, size_t ks, char* value, size_t vs);
void doRead(rbtree *tree, char* key,char* value);
#endif
