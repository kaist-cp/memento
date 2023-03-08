#ifndef HASHMAP_V_H
#define HASHMAP_V_H

#include <stddef.h>
#include <stdint.h>
#include <stddef.h>
#include <stdint.h>


struct entry {
	uint64_t key;
	char *value;

	/* next entry list pointer */
	struct entry *next;
};


struct hashmap_args {
	uint32_t seed;
};

enum hashmap_cmd {
	HASHMAP_CMD_REBUILD,
	HASHMAP_CMD_DEBUG,
};

struct hashmap_tx;

int hm_v_check(struct hashmap_tx *hashmap);
int hm_v_create(struct hashmap_tx **map, void *arg);
int hm_v_init(struct hashmap_tx *hashmap);
int hm_v_insert(struct hashmap_tx *hashmap,
		struct entry *ve, uint64_t key, char *value);
char *hm_v_remove(struct hashmap_tx *hashmap,
		uint64_t key);
char *hm_v_get(struct hashmap_tx *hashmap,
		uint64_t key);
int hm_v_lookup(struct hashmap_tx *hashmap,
		uint64_t key);
int hm_v_foreach(struct hashmap_tx *hashmap,
	int (*cb)(uint64_t key, char *value, void *arg), void *arg);
size_t hm_v_count(struct hashmap_tx *hashmap);
int hm_v_cmd(struct hashmap_tx *hashmap,
		unsigned cmd, uint64_t arg);

#endif /* HASHMAP_V_H */
