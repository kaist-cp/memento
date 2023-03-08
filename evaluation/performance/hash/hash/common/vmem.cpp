#include <hash_api.h>
#include <libvmem.h>
#include <stdlib.h>
unsigned long PM_POOL_SZ = 32UL * 1024 * 1024 * 1024;
VMEM* vmp = vmem_create(PMEM_LOC, PM_POOL_SZ);
void deletePM() { vmem_delete(vmp); }
void vmem_print() { vmem_stats_print(vmp, "1"); }