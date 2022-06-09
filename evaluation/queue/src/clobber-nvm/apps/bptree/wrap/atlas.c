#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include "../tree.h"

static BPTree *popRoot = NULL;

void init_context();
status_t PersistentTreeCreate(BPTree **tree) {
    init_context();
    if (popRoot != NULL) {
        *tree = popRoot;
        return Success;
    }
    return TreeCreate(tree);
}

void finalize_context();
status_t PersistentTreeDestroy(BPTree **tree) {
    // we do not destroy persistent trees
    *tree = NULL;
    finalize_context();
    return Success;
}

bool atlas_try_setting_root_ptr(void *ptr, size_t size) {
    if (popRoot != NULL || size != sizeof(BPTree)) return false;
    return __sync_bool_compare_and_swap(&popRoot, NULL, (BPTree *)ptr);
}

void atlas_set_root_ptr(void *ptr) {
    popRoot = (BPTree *)ptr;
}
