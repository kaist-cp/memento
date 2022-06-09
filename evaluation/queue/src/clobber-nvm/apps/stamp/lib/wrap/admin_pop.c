#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <libpmemobj.h>
#include "admin_pop.h"

void* get_baseptr(){
	return admin_pop;
}

void* init_admin_pop(){
	admin_pop = pmemobj_open(admin_path, LAYOUT);
	if (!admin_pop) {
		admin_pop = pmemobj_create(admin_path, LAYOUT, POOL_SIZE, 0777);
		if (!admin_pop) {
			printf("Error: failed to create a pool at %s (%lu): %s\n", admin_path, (size_t)POOL_SIZE, strerror(errno));
			exit(1);
		}
	} else {
	}

	root = POBJ_ROOT(admin_pop, struct list_head);
	assert(!TOID_IS_NULL(root));
	return admin_pop;
}


void add_node(uint64_t offset){
	pmemobj_mutex_lock(admin_pop, &D_RW(root)->lock);

	TX_BEGIN(admin_pop) {
		TOID(struct list_elem) tail = D_RW(root)->tail;
		TOID(struct list_elem) node;
		node = TX_ZNEW(struct list_elem);

		D_RW(node)->funcptr_offset = offset;
		TX_ADD(root);

		if (!TOID_IS_NULL(tail)) {
			TX_ADD(tail);
			D_RW(tail)->next = node;
		} else {
			D_RW(root)->head = node;
		}

		D_RW(root)->tail = node;
		D_RW(root)->num_elements++;

	} TX_END

	pmemobj_mutex_unlock(admin_pop, &D_RW(root)->lock);
}



int get_all_elem(uint64_t* offset){
	TOID(struct list_elem) tmp = D_RO(root)->head;
	int i=0;
	while (!TOID_IS_NULL(tmp)) {
		offset[i] = D_RO(tmp)->funcptr_offset;
		i++;
		tmp = D_RO(tmp)->next;
	}
	return i;
}

void scan_all_offset(){
	TOID(struct list_elem) tmp = D_RO(root)->head;
	while (!TOID_IS_NULL(tmp)) {
		uint64_t offset = D_RO(tmp)->funcptr_offset;
		//TODO: recovery code
		printf("%p ", offset);
		tmp = D_RO(tmp)->next;
	}
	printf("\n");
}

void admin_pop_close(){
	// pool deletion
	pmemobj_close(admin_pop);
}

int admin_pop_check(){
	int ret = 0;
	// pool consistency check
	ret = pmemobj_check(admin_path, LAYOUT);
	if (ret == 1) {
		printf("Pool %s is consistent.\n", admin_path);
	} else if (ret == 0) {
		printf("Error: pool is not consistent.\n");
		exit(1);
	} else {
		printf("Error: pmemobj_check failed: %s\n", strerror(errno));
		exit(1);
	}

	return 0;
}


