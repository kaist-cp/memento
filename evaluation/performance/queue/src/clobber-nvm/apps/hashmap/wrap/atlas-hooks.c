#include <stdint.h>
#include "../benchmark.h"

void init_atlas();
int __wrap_hashmap_recover(struct hashmap_root **ptr) {
    init_atlas();
    return 1; // we do not recover Atlas
}

void finalize_atlas();
void __wrap_hashmap_close(struct hashmap_root *rootPtr) {
    finalize_atlas();
}

void dumpstats(){}
