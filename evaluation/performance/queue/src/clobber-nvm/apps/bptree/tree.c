#include <string.h>
#include <malloc.h>
#include <assert.h>
#include <stdbool.h>
#include "tree.h"

status_t TreeAllocate(BPTree *tree) {
    size_t levelSize = 1;
    size_t totalINodes = 0;
    for (unsigned i = 1; i < TREE_MAX_HEIGHT; i++) {
        totalINodes += levelSize;
        levelSize *= TREE_BRANCH_FACTOR;
    }
    // internal nodes (TreeINode)
    void *iNodes = calloc(totalINodes, sizeof(TreeINode));
    if (iNodes == NULL) return Failed;
    memset(iNodes, 0, totalINodes * sizeof(TreeINode));
    tree->iNodes = (TreeINode *)iNodes;
    tree->iNodesSize = totalINodes;
    tree->iNodesUsed = 0;
    tree->iNodesHeight = 0;
    for (size_t i = 0; i < totalINodes; i++) {
        pthread_rwlock_init(&tree->iNodes[i].lock, NULL);
    }

    // parent of leaf nodes (TreePLNode)
    tree->plNodesSize = levelSize;
    tree->plNodesUsed = 0;
    void *plNodes = calloc(levelSize, sizeof(TreePLNode));
    if (plNodes == NULL) return Failed;
    memset(plNodes, 0, levelSize * sizeof(TreePLNode));
    tree->plNodes = (TreePLNode *)plNodes;

    for (size_t i = 0; i < levelSize; i++) {
        pthread_rwlock_init(&tree->plNodes[i].lock, NULL);
    }

    return Success;
}

status_t TreeCreate(BPTree **tree) {
    pthread_rwlock_t dummy;
    pthread_rwlock_init(&dummy, NULL);

    pthread_rwlock_wrlock(&dummy);
    BPTree *newTree = (BPTree*)malloc(sizeof(BPTree));
    if (newTree == NULL) return Failed;
    if (TreeAllocate(newTree) != Success) {
        pthread_rwlock_unlock(&dummy);
        return Failed;
    }
    newTree->totalNodes = 0;
    newTree->leafs = NULL;
	pthread_rwlock_init(&newTree->lock0, NULL);
    pthread_rwlock_init(&newTree->lock, NULL);
    *tree = newTree;
    pthread_rwlock_unlock(&dummy);
    return Success;
}

status_t TreeDestroy(BPTree **tree) {
    pthread_rwlock_t dummy;
    pthread_rwlock_init(&dummy, NULL);

    pthread_rwlock_wrlock(&dummy);
    free((*tree)->iNodes);
    free((*tree)->plNodes);
    while ((*tree)->leafs != NULL) {
        TreeLNode *leaf = (*tree)->leafs;
        free(leaf);
        (*tree)->leafs = (*tree)->leafs->next; // TODO loop invariant, must log once outside
    }
    free(*tree);
    *tree = NULL;
    pthread_rwlock_unlock(&dummy);

    return Success;
}

// NOTE: external calls must always pass inodeID = TREE_ROOT
uint32_t FindPLNode(BPTree *tree, uint32_t inodeID, const char *key) {
    if (inodeID == TREE_ROOT) {
        while (1) {
            TreeINode *root = &GetINode(tree, inodeID);
            uint32_t id = root->id;
            uint32_t ver = root->version; // TODO already swizzled
            pthread_rwlock_rdlock(&root->lock); // TODO already swizzled
            if (root->version != ver || root->id != id) { // TODO already swizzled
                pthread_rwlock_unlock(&root->lock); // TODO already swizzled
                continue;
            }
            break;
        }
    }

    if (!isPLNode(tree, inodeID)) {
        TreeINode *node = &GetINode(tree, inodeID);
        if (inodeID != TREE_ROOT) pthread_rwlock_rdlock(&node->lock);
        uint8_t usedKeys = node->usedKeys;
        for (uint8_t i = 0; i < usedKeys; i++) {
            if (KeysLTE(key, node->keys[i])) { // TODO coalescing for swizzling callbacks
                return FindPLNode(tree, LMChild(inodeID) + i, key);
            }
        }

        return FindPLNode(tree, LMChild(inodeID) + node->usedKeys, key);
    }

    uint32_t plnID = INodeIDToPLNodeID(tree, inodeID);
    if (plnID < PLNodesSize(tree)) {
        pthread_rwlock_rdlock(&GetPLNode(tree, plnID).lock);
        return plnID;
    }

    return TREE_NIL;
}

uint32_t HandleEmptyTree(BPTree *tree, const char *key) {
    TreeINode *root = &GetINode(tree, TREE_ROOT);

    uint32_t root_id = root->id;
    uint32_t root_ver = root->version;

    pthread_rwlock_unlock(&(root->lock));
    pthread_rwlock_wrlock(&(root->lock)); // promote lock

    tree->iNodesUsed = 1;
    tree->iNodesHeight = 1;
    tree->plNodesSize = tree->plNodesSize; // dummy, remove after fixing coalescing
    PLNodesSize(tree) = TREE_BRANCH_FACTOR;
    tree->plNodes = tree->plNodes; // dummy, remove after fixing coalescing

    if (root->id != root_id || root->version != root_ver) {
        pthread_rwlock_unlock(&(root->lock));
        return TREE_NIL;
    }

    strcpy(root->keys[0], key);
    root->id = __sync_fetch_and_add(&tree->totalNodes, 1);
    root->version = root->version + 1;
    root->usedKeys = 1;

    pthread_rwlock_rdlock(&GetPLNode(tree, 0).lock);
    GetPLNode(tree, 0).id = __sync_fetch_and_add(&tree->totalNodes, 1);
    GetPLNode(tree, 1).id = __sync_fetch_and_add(&tree->totalNodes, 1);

    return 0; // first PLNode
}

uint32_t GetPLNodeParent(BPTree *tree, uint32_t plnID) {
    uint32_t parentOffset = plnID / TREE_BRANCH_FACTOR;
    uint64_t firstINodeLeafIndex = 0;
    uint64_t levelSize = 1;
    for (uint8_t i = 1; i < INodeHeight(tree); i++) {
        firstINodeLeafIndex += levelSize;
        levelSize *= TREE_BRANCH_FACTOR;
    }
    return firstINodeLeafIndex + parentOffset;
}

void ReleaseLocks(BPTree *tree, uint32_t plnID) {
    TreePLNode *pln = &GetPLNode(tree, plnID);
    pthread_rwlock_unlock(&pln->lock);

    uint32_t inodeID = GetPLNodeParent(tree, plnID);
    do {
        TreeINode *node = &GetINode(tree, inodeID);
        pthread_rwlock_unlock(&node->lock);
        inodeID = INodeParent(inodeID);
    } while (inodeID != TREE_NIL);
}

void ReleaseINodeLocks(BPTree *tree, uint32_t inodeID) {
    pthread_rwlock_unlock(&GetINode(tree, inodeID).lock);
    while (inodeID != TREE_ROOT) {
        inodeID = INodeParent(inodeID);
        pthread_rwlock_unlock(&GetINode(tree, inodeID).lock);
    }
}

status_t AdvanceTreeHeight(BPTree *tree) {
    if (INodeHeight(tree) == TREE_MAX_HEIGHT - 1) {
        fprintf(stderr, "already reached maximum height!\n");
        return Failed;
    }

    uint32_t firstNodeAtLevel = 0;
    uint32_t levelSize = 1;
    for (uint8_t i = 1; i < INodeHeight(tree); i++) {
        firstNodeAtLevel += levelSize;
        levelSize *= TREE_BRANCH_FACTOR;
    }

    uint32_t lastLevelSize = levelSize;
    while (levelSize > 0) {
        TreeINode *src = &GetINode(tree, firstNodeAtLevel);
        TreeINode *dst = &GetINode(tree, firstNodeAtLevel + levelSize);
        memcpy(dst, src, levelSize * sizeof(TreeINode));
        levelSize /= TREE_BRANCH_FACTOR;
        firstNodeAtLevel -= levelSize;
    }

    INodesSize(tree) = INodesSize(tree) + lastLevelSize * TREE_BRANCH_FACTOR;
    INodeHeight(tree) = INodeHeight(tree) + 1;
    tree->plNodesSize = tree->plNodesSize; // dummy, remove after fixing coalescing
    PLNodesSize(tree) = PLNodesSize(tree) * TREE_BRANCH_FACTOR;
    tree->plNodes = tree->plNodes; // dummy, remove after fixing coalescing
    tree->totalNodes = tree->totalNodes; // dummy, remove after fixing coalescing

    TreeINode *newRoot = &GetINode(tree, 0); // TREE_ROOT == 0
    memset(newRoot->keys, 0, sizeof(newRoot->keys)); // TODO must coalesce with previous too
    newRoot->id = __sync_fetch_and_add(&tree->totalNodes, 1);
    newRoot->version = 0;
    newRoot->usedKeys = 0;

    TreeINode *oldRoot = &GetINode(tree, 1);
    pthread_rwlock_init(&oldRoot->lock, NULL);

    return Success;
}

status_t MoveNode(BPTree *tree, uint32_t srcIdx, uint32_t dstIdx) {
    // Move TreePLNode(s)
    if (isPLNode(tree, srcIdx)) {
        if (!isPLNode(tree, dstIdx)) return Failed;
        TreePLNode *src = &GetPLNode(tree, INodeIDToPLNodeID(tree, srcIdx));
        TreePLNode *dst = &GetPLNode(tree, INodeIDToPLNodeID(tree, dstIdx));
        memcpy(dst, src, sizeof(TreePLNode));
        return Success;
    }

    if (GetINode(tree, srcIdx).usedKeys == 0) return Success;

    // Recursively move sub-trees
    uint32_t srcLMChildIdx = LMChild(srcIdx);
    uint32_t dstLMChildIdx = LMChild(dstIdx);
    for (uint8_t i = 0; i <= GetINode(tree, srcIdx).usedKeys; i++) {
        status_t s = MoveNode(tree, srcLMChildIdx + i, dstLMChildIdx + i);
        if (s != Success) return s;
    }

    memcpy(&GetINode(tree, dstIdx), &GetINode(tree, srcIdx), sizeof(TreeINode));
    return Success;
}

status_t SplitINode(BPTree *tree, uint32_t id) {
    TreeINode *node = &GetINode(tree, id);
    uint32_t oldID = id;

    if (id == TREE_ROOT) {
        uint32_t nodeID = node->id;
        uint32_t nodeVer = node->version;
        pthread_rwlock_unlock(&node->lock);
        pthread_rwlock_wrlock(&node->lock); // promote lock
        if (node->id != nodeID || node->version != nodeVer) {
            pthread_rwlock_unlock(&node->lock);
            return TryAgain;
        }
        if (AdvanceTreeHeight(tree) != Success) {
            pthread_rwlock_unlock(&node->lock);
            return Failed;
        }
        id = LMChild(TREE_ROOT);
        node = &GetINode(tree, id);
    }

    uint32_t parentID = INodeParent(id);
    TreeINode *parent = &GetINode(tree, parentID);

    if (parent->usedKeys == TREE_BRANCH_FACTOR - 1) {
        pthread_rwlock_unlock(&node->lock);
        return SplitINode(tree, parentID);
    }

    if (oldID != TREE_ROOT) {
        uint32_t pID = parent->id;
        uint32_t pVer = parent->version;
        pthread_rwlock_unlock(&node->lock);
        pthread_rwlock_unlock(&parent->lock);
        pthread_rwlock_wrlock(&parent->lock);
        if (parent->id != pID || parent->version != pVer) {
            ReleaseINodeLocks(tree, parentID);
            return TryAgain;
        }
    }

    // Make room for split (relocate right siblings)
    uint32_t lastSib = LMChild(parentID) + parent->usedKeys;
    for (uint32_t s = lastSib; s > id; s--) MoveNode(tree, s, s + 1);

    TreeINode *newSib = &GetINode(tree, id + 1);
    newSib->id = __sync_fetch_and_add(&tree->totalNodes, 1);
    newSib->version = 0;
    uint8_t med = (TREE_BRANCH_FACTOR - 1) / 2;
    char *medKey = node->keys[med];

    // Assign half of the sub-trees to the new sibling
    uint32_t rmChild = LMChild(id) + TREE_BRANCH_FACTOR - 1;
    uint32_t rmChildOfNewSib = LMChild(id + 1) + (TREE_BRANCH_FACTOR / 2) - 1;
    for (uint8_t i = 0; i < TREE_BRANCH_FACTOR / 2; i++) {
        MoveNode(tree, rmChild--, rmChildOfNewSib--);
    }

    node->usedKeys = med;
    newSib->usedKeys = (TREE_BRANCH_FACTOR - 1) - med - 1;

    for (uint8_t i = 0; i < TREE_BRANCH_FACTOR / 2; i++) {
        memcpy(newSib->keys[i], node->keys[med + i + 1], TREE_MAX_KEY_LENGTH); // TODO loop coalescing
    }

    uint8_t keyIdx = id - LMChild(parentID);
    for (int16_t i = parent->usedKeys - 1; i >= (int16_t)keyIdx; i--) {
        memcpy(parent->keys[i + 1], parent->keys[i], TREE_MAX_KEY_LENGTH); // TODO loop coalescing
    }
    memcpy(parent->keys[keyIdx], medKey, TREE_MAX_KEY_LENGTH);
    parent->version = parent->version + 1;
    parent->usedKeys = parent->usedKeys + 1;

    ReleaseINodeLocks(tree, parentID);
    return Success;
}

status_t SplitPLNode(BPTree *tree, uint32_t id) {
    uint32_t parentID = GetPLNodeParent(tree, id);
    TreeINode *parent = &GetINode(tree, parentID);

    pthread_rwlock_unlock(&GetPLNode(tree, id).lock);

    if (parent->usedKeys == TREE_BRANCH_FACTOR - 1) {
        return SplitINode(tree, parentID);
    }

    uint32_t pID = parent->id;
    uint32_t pVer = parent->version;
    pthread_rwlock_unlock(&parent->lock);
    pthread_rwlock_wrlock(&parent->lock);
    if (parent->id != pID || parent->version != pVer) {
        ReleaseINodeLocks(tree, parentID);
        return TryAgain;
    }

    uint8_t oldSibUsedKeys = GetPLNode(tree, id + 1).usedKeys;

    // make room for the new node
    uint32_t sib = PLNodeLMS(id) + parent->usedKeys;
    while (sib != id) {
        memcpy(&GetPLNode(tree, sib + 1), &GetPLNode(tree, sib),
                sizeof(TreePLNode));
        sib--;
    }

    // split the node
    TreePLNode *src = &GetPLNode(tree, id);
    TreePLNode *dst = &GetPLNode(tree, id + 1);
    src->version = src->version + 1;
    dst->id = __sync_fetch_and_add(&tree->totalNodes, 1);
    dst->version = 0;

    uint8_t med = TREE_BRANCH_FACTOR / 2;
    for (uint8_t i = med; i < TREE_BRANCH_FACTOR; i++) {
        memcpy(dst->keys[i - med], src->keys[i], TREE_MAX_KEY_LENGTH); // TODO loop coalescing
        dst->leafs[i - med] = src->leafs[i]; // TODO loop coalescing
    }
    src->usedKeys = med;
    dst->usedKeys = TREE_BRANCH_FACTOR - med;

    // update the parent INode
    uint8_t parentUsedKeys = parent->usedKeys;
    if (parent->usedKeys == 1 && id == PLNodeLMS(id) && oldSibUsedKeys == 0) {
        memcpy(parent->keys[0], src->keys[med - 1], TREE_MAX_KEY_LENGTH);
    }
    else {
        uint8_t idx = id % TREE_BRANCH_FACTOR;
        for (int16_t i = parent->usedKeys - 1; i >= idx; i--) {
            memcpy(parent->keys[i + 1], parent->keys[i], TREE_MAX_KEY_LENGTH); // TODO loop coalescing
        }
        memcpy(parent->keys[idx], src->keys[med - 1], TREE_MAX_KEY_LENGTH); // TODO coalesce with the previous
        parentUsedKeys++;
    }
    parent->version = parent->version + 1;
    parent->usedKeys = parentUsedKeys;

    ReleaseINodeLocks(tree, parentID);
    return Success;
}

uint32_t FindOrCreatePLNode(BPTree *tree, const char *key) {
    uint32_t parentID = TREE_NIL;
    while (1) {
        parentID = FindPLNode(tree, TREE_ROOT, key);
        if (parentID == TREE_NIL) {
            parentID = HandleEmptyTree(tree, key);
            if (parentID == TREE_NIL) continue;
        }

        if (GetPLNode(tree, parentID).usedKeys == TREE_BRANCH_FACTOR) {
            status_t st = SplitPLNode(tree, parentID);
            if (st != Success) {
                parentID = TREE_NIL;
                if (st != TryAgain) break;
            }
        }
        else {
            TreePLNode *parent = &GetPLNode(tree, parentID);
            uint32_t id = parent->id;
            uint32_t ver = parent->version;
            pthread_rwlock_unlock(&parent->lock);
            pthread_rwlock_wrlock(&parent->lock); // promote lock

            if (parent->id != id || parent->version != ver ||
                    parent->usedKeys == TREE_BRANCH_FACTOR) {
                ReleaseLocks(tree, parentID);
                parentID = TREE_NIL; // Try again
                continue;
            }
            break;
        }
    }
    return parentID;
}

TreeLNode *AllocLeaf(const char *key, const char *value) {
    size_t leafSize = sizeof(TreeLNode);
    TreeLNode *leaf = (TreeLNode *)memalign(CACHE_LINE_SIZE, leafSize);
    if (leaf != NULL) {
        pthread_rwlock_init(&leaf->lock, NULL);
        strcpy(leaf->key, key);
        size_t valueSize = strlen(value);
        char *valueBuffer = (char *)memalign(CACHE_LINE_SIZE, valueSize);
        memcpy(valueBuffer, value, valueSize + 1);
        leaf->value = valueBuffer;
        leaf->next = NULL;
    }
    return leaf;
}

TreeLNode *GetRightMostLeaf(BPTree *tree, uint32_t nodeID) {
    TreeLNode *leaf = NULL;

    if (isPLNode(tree, nodeID)) {
        TreePLNode *node = &GetPLNode(tree, INodeIDToPLNodeID(tree, nodeID));
        pthread_rwlock_rdlock(&node->lock);
        if (node->usedKeys > 0) {
            leaf = node->leafs[node->usedKeys - 1];
        }
        pthread_rwlock_unlock(&node->lock);
    }
    else {
        TreeINode *node = &GetINode(tree, nodeID);
        pthread_rwlock_rdlock(&node->lock);
        uint32_t fc = LMChild(nodeID);
        for (int16_t i = node->usedKeys; leaf == NULL && i >= 0; i--) {
            leaf = GetRightMostLeaf(tree, fc + i);
        }
        pthread_rwlock_unlock(&node->lock);
    }

    return leaf;
}

TreeLNode *PrevLeaf(BPTree *tree, uint32_t parentID, uint8_t keyIdx) {
    TreeLNode *prev = NULL;

    if (keyIdx > 0) {
        prev = GetPLNode(tree, parentID).leafs[keyIdx - 1];
    }
    else if (parentID % TREE_BRANCH_FACTOR != 0) {
        uint32_t pLMSib = PLNodeLMS(parentID);
        uint8_t pINodeIdx = parentID % TREE_BRANCH_FACTOR;
        for (int16_t i = pINodeIdx - 1; prev == NULL && i >= 0; i--) {
            TreePLNode *sib = &GetPLNode(tree, pLMSib + i);
            pthread_rwlock_rdlock(&sib->lock);
            if (sib->usedKeys > 0) {
                prev = sib->leafs[sib->usedKeys - 1];
            }
            pthread_rwlock_unlock(&sib->lock);
        }
    }
    else { // expand the search domain
        uint32_t iNodeID = GetPLNodeParent(tree, parentID);
        while (prev == NULL && iNodeID != TREE_ROOT) {
            int16_t prevSibCount = iNodeID - INodeLMS(iNodeID);
            while (prevSibCount-- > 0) {
                prev = GetRightMostLeaf(tree, INodeLMS(iNodeID) + prevSibCount);
                if (prev != NULL) break;
            }
            iNodeID = INodeParent(iNodeID);
        }
    }

    return prev;
}

status_t AddLeaf(BPTree *tree, TreeLNode *node, TreeLNode *prev) {
    status_t s = Success;

    if (prev == NULL) { // updating head
        pthread_rwlock_wrlock(&tree->lock);
        if (tree->leafs == NULL) { // empty tree
            tree->leafs = node;
        }
        else {
            assert(KeysLTE(node->key, tree->leafs->key));
            node->next = tree->leafs;
            tree->leafs = node;
        }
        pthread_rwlock_unlock(&tree->lock);
    }
    else {
        pthread_rwlock_wrlock(&prev->lock);
        if (prev->next == NULL || KeysLTE(node->key, prev->next->key)) {
            node->next = prev->next;
            prev->next = node;
        }
        else {
            s = TryAgain;
        }
        pthread_rwlock_unlock(&prev->lock);
    }

    return s;
}

status_t TreeInsert(BPTree *tree, const char *key, size_t keysize, const char *value, size_t valuesize) {
    size_t keySize = strlen(key);
    if (keySize >= TREE_MAX_KEY_LENGTH) {
        fprintf(stderr, "key-size is longer that maximum key length!\n");
        return Failed;
    }
    uint32_t parentID = FindOrCreatePLNode(tree, key);
    if (parentID == TREE_NIL) {
        fprintf(stderr, "failed to find/create a parent for the new pair\n");
        return Failed;
    }

    TreePLNode *parent = &GetPLNode(tree, parentID);
    uint8_t pos = parent->usedKeys;

    // TODO possible opt opportunity for loop coalescing
    while (pos > 0 && KeysGT(parent->keys[pos - 1], key)) {
        memcpy(parent->keys[pos], parent->keys[pos - 1], TREE_MAX_KEY_LENGTH);
        parent->leafs[pos] = parent->leafs[pos - 1];
        pos--;
    }

    memset(parent->keys[pos], 0, TREE_MAX_KEY_LENGTH);
    memcpy(parent->keys[pos], key, keySize);
    parent->usedKeys = parent->usedKeys + 1;

    status_t status = Success;
    TreeLNode *obj = AllocLeaf(key, value);
    parent->leafs[pos] = obj;


    if (obj != NULL) {
        pthread_rwlock_wrlock(&obj->lock);
        TreeLNode *prev = NULL;
        while (true) {
            prev = PrevLeaf(tree, parentID, pos);
            status = AddLeaf(tree, obj, prev);
            if (status == Success) break;
            else if (status == TryAgain) continue;
            else {
                fprintf(stderr, "unable to append to the linked-list\n");
                status = Failed;
            }
        }
        pthread_rwlock_unlock(&obj->lock);
    }
    else {
        fprintf(stderr, "cannot allocate memory for the new pair\n");
        status = Failed;
    }

    ReleaseLocks(tree, parentID);
    return status;
}

typedef TreeLNode * (*CustomReadFunc)(TreeLNode *);

status_t TreeCustomRead(BPTree *tree, const char *key,
        char *oKey, char *oVal, CustomReadFunc func) {
    size_t keySize = strlen(key);
    if (keySize >= TREE_MAX_KEY_LENGTH) return Failed;

    uint32_t parentIdx = FindPLNode(tree, TREE_ROOT, key);
    if (parentIdx == TREE_NIL) return NotFound;

    status_t s = NotFound;
    TreePLNode *pln = &GetPLNode(tree, parentIdx);
    uint8_t usedKeys = pln->usedKeys;
    for (uint8_t i = 0; i < usedKeys; i++) {
        if (KeysEQ(key, pln->keys[i])) { // TODO use swizzled
            TreeLNode *leaf = pln->leafs[i]; // TODO use swizzled
            pthread_rwlock_rdlock(&leaf->lock);
            if (func != NULL) leaf = func(leaf);
            if (leaf != NULL) {
                if (oKey != NULL) strcpy(oKey, leaf->key);
                if (oVal != NULL) strcpy(oVal, leaf->value);
                pthread_rwlock_unlock(&leaf->lock);
                s = Success;
            }
            break;
        }
    }

    ReleaseLocks(tree, parentIdx);
    return s;

}

status_t TreeRead(BPTree *tree, const char *key, size_t keysize, char *value, size_t valuesize) {
    return TreeCustomRead(tree, key, NULL, value, NULL);
}

TreeLNode *ReadNextLeaf(TreeLNode *leaf) {
    TreeLNode *next = leaf->next;
    pthread_rwlock_unlock(&leaf->lock);
    if (next != NULL) pthread_rwlock_rdlock(&next->lock);
    return next;
}
status_t TreeReadNext(BPTree *tree, const char *key, char *nKey, char *nValue) {
    return TreeCustomRead(tree, key, nKey, nValue, ReadNextLeaf);
}


status_t UpdateLeaf(TreeLNode *leaf, const char *value, size_t value_size){
	status_t s = Success;
            pthread_rwlock_wrlock(&leaf->lock);
            size_t valueSize = strlen(value);
            if (strlen(leaf->value) == valueSize) {
                strcpy(leaf->value, value);
            }
            else {
                // TODO store buffer size for the value
                free(leaf->value);
                char* newValueBuffer = (char *)memalign(CACHE_LINE_SIZE, valueSize);
                if (newValueBuffer == NULL) s = Failed;
                else memcpy(newValueBuffer, value, valueSize);
                leaf->value = newValueBuffer;
            }
            pthread_rwlock_unlock(&leaf->lock);
	return s;
}

/*
// TODO refactor (merge) update and read
status_t TreeUpdate(BPTree *tree, size_t treesize, const char *key, size_t keysize, const char *value, size_t valuesize) {
    size_t keySize = strlen(key);
    if (keySize >= TREE_MAX_KEY_LENGTH) return Failed;
    if (value == NULL) return Failed;

    uint32_t parentIdx = FindPLNode(tree, TREE_ROOT, key);
    if (parentIdx == TREE_NIL) return NotFound;

    status_t s = NotFound;
    TreePLNode *pln = &GetPLNode(tree, parentIdx);
    for (uint8_t i = 0; i < pln->usedKeys; i++) {
        if (KeysEQ(key, pln->keys[i])) {
            TreeLNode *leaf = pln->leafs[i];
            s = Success;
	    s = UpdateLeaf(leaf, value, strlen(value));
            break;
        }
    }

    ReleaseLocks(tree, parentIdx);
    return s;
}
*/
status_t TreeDelete(BPTree *tree, const char *key) {
    // TODO
    return Success;
}


