#include "tree.h"

status_t TreeUpdate(BPTree *tree, const char *key, size_t keysize, const char *value, size_t valuesize) {
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

