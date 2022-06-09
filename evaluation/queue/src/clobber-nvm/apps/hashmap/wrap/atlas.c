#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include "../benchmark.h"

struct hashmap_root *popRoot = NULL;

void init_context();
int __wrap_hashmap_recover(struct hashmap_root **ptr) {
    init_context();
    if (popRoot == NULL) return 1;
    *ptr = popRoot;
    return 0;
}

void finalize_context();
void __wrap_hashmap_close(struct hashmap_root *rootPtr) {
    finalize_context();
}

bool atlas_try_setting_root_ptr(void *ptr, size_t size) {
    if (popRoot != NULL || size != sizeof(struct hashmap_root)) return false;
    return __sync_bool_compare_and_swap(&popRoot, NULL, (struct hashmap_root *)ptr);
}

void atlas_set_root_ptr(void *ptr) {
    popRoot = (struct hashmap_root *)ptr;
}
