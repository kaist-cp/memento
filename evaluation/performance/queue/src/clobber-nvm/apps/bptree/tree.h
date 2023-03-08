#ifndef HOOKS_TREE_H
#define HOOKS_TREE_H
#include "common.h"


#define isPLNode(tree, node_id) \
    (node_id >= tree->iNodesUsed)
#define INodeIDToPLNodeID(tree, in_id) \
    (in_id - tree->iNodesUsed)
#define GetINode(tree, id) \
    (tree->iNodes[id])
#define INodesSize(tree) \
    (tree->iNodesUsed)
#define LMChild(in_id) \
    ((in_id) * TREE_BRANCH_FACTOR + 1)
#define INodeLMS(in_id) \
    (in_id - ((in_id - 1) % TREE_BRANCH_FACTOR))
#define INodeParent(in_id) \
    (in_id > 0 ? ((INodeLMS(in_id) - 1) / TREE_BRANCH_FACTOR) : TREE_NIL)
#define INodeHeight(tree) \
    (tree->iNodesHeight)
#define GetPLNode(tree, id) \
    (tree->plNodes[id])
#define PLNodesSize(tree) \
    (tree->plNodesUsed)
#define PLNodeLMS(pln_id) \
    (pln_id - (pln_id % TREE_BRANCH_FACTOR))
#define KeysEQ(k1, k2) \
    (strcmp(k1, k2) == 0)
#define KeysLTE(k1, k2) \
    (strcmp(k1, k2) <= 0)
#define KeysGT(k1, k2) \
    (strcmp(k1, k2) > 0)


typedef struct {
#ifdef KAMINO
    uint64_t dirty;
#endif
    KeyType keys[TREE_BRANCH_FACTOR - 1];
    uint32_t id;
    uint32_t version;
    uint8_t usedKeys;
    pthread_rwlock_t lock;
} TreeINode; // internal node

typedef struct TreeLNode {
#ifdef KAMINO
    uint64_t dirty;
#endif
    KeyType key;
    char *value;
    struct TreeLNode *next; // pointer to the right sibling
    pthread_rwlock_t lock;
} TreeLNode; // leaf node

typedef struct {
#ifdef KAMINO
    uint64_t dirty;
#endif
    KeyType keys[TREE_BRANCH_FACTOR];
    uint32_t id;
    uint32_t version;
    uint8_t usedKeys;
    TreeLNode *leafs[TREE_BRANCH_FACTOR];
    pthread_rwlock_t lock;
} TreePLNode; // parent of leaf-nodes

typedef struct {
#ifdef KAMINO
    uint64_t dirty;
#endif
    TreeINode *iNodes;
    size_t iNodesSize;
    size_t iNodesUsed;
    size_t iNodesHeight;

    size_t plNodesSize;
    size_t plNodesUsed;
    TreePLNode *plNodes;

    size_t totalNodes;
    TreeLNode *leafs;
    pthread_rwlock_t lock;
	pthread_rwlock_t lock0;
} BPTree;

status_t TreeCreate(BPTree **tree);

status_t TreeDestroy(BPTree **tree);

status_t TreeInsert(BPTree *tree, const char *key, size_t keysize, const char *value, size_t valuesize);

status_t TreeRead(BPTree *tree, const char *key, size_t keysize, char *value, size_t valuesize);

status_t TreeReadNext(BPTree *tree, const char *key, char *nKey, char *nValue);

status_t TreeUpdate(BPTree *tree, const char *key, size_t keysize, const char *value, size_t valuesize);

status_t TreeDelete(BPTree *tree, const char *key);

/*************************/
uint32_t FindPLNode(BPTree *tree, uint32_t inodeID, const char *key);
void ReleaseLocks(BPTree *tree, uint32_t plnID);
status_t UpdateLeaf(TreeLNode *leaf, const char *value, size_t value_size);
#endif // HOOKS_TREE_H

