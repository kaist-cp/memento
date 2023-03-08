#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <string.h>

#include "hashmap_v.h"
//#include "../../pmdk-1.6/src/examples/libpmemobj/hashmap/hashmap_internal.h"
//#include "../../../mnemosyne-gcc/usermode/examples/simple/hashmap_internal.h"
#include "hashmap_internal.h"

#define VALUE_SIZE 256
#define VALUE_COUNT 600*8 /* VALUE_COUNT * BUCKET >= 100000 0 */

/*
struct entry {
	uint64_t key;
	char *value;

	struct entry *next;
};
*/

struct buckets {
	/* number of buckets */
	size_t nbuckets;
	/* array of lists */
	struct entry *bucket[];
};

struct hashmap_tx {
    uint32_t reserved; // avoid structure packing
	/* random number generator seed */
	uint32_t seed;

	/* hash function coefficients */
	uint32_t hash_fun_a;
	uint32_t hash_fun_b;
	uint64_t hash_fun_p;

	/* number of values inserted */
	uint64_t count;

	/* buckets */
	struct buckets *buckets;

	void* valueaddr;
	void* entryaddr;
};

inline void on_nvmm_read(void *ptr, size_t size) {}

/*
 * create_hashmap -- hashmap initializer
 */
static void
create_hashmap(struct hashmap_tx *hashmap, uint32_t seed)
{
	size_t len = INIT_BUCKETS_NUM;
	size_t sz = sizeof(struct buckets) +
			len * sizeof(struct entry*);

    uint32_t hash_fun_a = 0;
    do {
        hash_fun_a = (uint32_t)rand();
    } while (hash_fun_a == 0);
    hashmap->seed = seed;
    hashmap->hash_fun_a = hash_fun_a;
    hashmap->hash_fun_b = (uint32_t)rand();
    hashmap->hash_fun_p = HASH_FUNC_COEFF_P;
    hashmap->count = 0;

    struct buckets *bucket = (struct buckets *)malloc(sz);
    hashmap->buckets = bucket;
    memset(bucket, 0, sz);
    bucket->nbuckets = len;

	hashmap->entryaddr = malloc(VALUE_COUNT * sizeof(struct entry));
	hashmap->valueaddr = malloc(VALUE_COUNT * VALUE_SIZE);
}

/*
 * hash -- the simplest hashing function,
 * see https://en.wikipedia.org/wiki/Universal_hashing#Hashing_integers
 */
static uint64_t
hash(const struct hashmap_tx *hashmap,
	const struct buckets *buckets, uint64_t value)
{
	uint32_t a = hashmap->hash_fun_a;
	uint32_t b = hashmap->hash_fun_b;
	uint64_t p = hashmap->hash_fun_p;
	size_t len = buckets->nbuckets;

	return ((a * value + b) % p) % len;
}

static void
hm_v_rebuild(struct hashmap_tx *hashmap, size_t new_len)
{
	struct buckets *buckets_old = hashmap->buckets;

	if (new_len == 0)
		new_len = buckets_old->nbuckets;

	size_t sz_new = sizeof(struct buckets) +
			new_len * sizeof(struct entry*);

    struct buckets *buckets_new = (struct buckets*)malloc(sz_new);
    memset(buckets_new, 0, sz_new);
    buckets_new->nbuckets = new_len;

    size_t nbuckets = buckets_old->nbuckets;
    for (size_t i = 0; i < nbuckets; ++i) {
        while (buckets_old->bucket[i] != NULL) {
            struct entry *en = buckets_old->bucket[i];
            uint64_t h = hash(hashmap, buckets_new, en->key);

            buckets_old->bucket[i] = en->next;

            en->next = buckets_new->bucket[h]; // TODO alias analysis
            buckets_new->bucket[h] = en;
        }
    }

    hashmap->buckets = buckets_new;
    free(buckets_old);
}

int
hm_v_insert(struct hashmap_tx *hashmap,
	struct entry *ve, uint64_t key, char *value)
{
/*
	struct buckets *buckets = hashmap->buckets;
	struct entry *var;

	uint64_t h = hash(hashmap, buckets, key);
	int num = 0;

	for (var = buckets->bucket[h]; var != NULL; var = var->next) {
		if (var->key == key)
			return 1;
		num++;
	}

	int ret = 0;
    struct entry *e = (struct entry*)malloc(sizeof(struct entry));
    e->key = key;
    e->value = value;
    e->next = buckets->bucket[h];
    buckets->bucket[h] = e;

    hashmap->count++;
    num++;

	if (ret)
		return ret;

	if (num > MAX_HASHSET_THRESHOLD ||
			(num > MIN_HASHSET_THRESHOLD &&
			hashmap->count > 2 * buckets->nbuckets))
		hm_v_rebuild(hashmap, buckets->nbuckets * 2);

	return 0;
*/

	struct buckets *buckets = hashmap->buckets;
	struct entry *var;

	uint64_t h = hash(hashmap, buckets, key);
	int num = 0;

	for (var = buckets->bucket[h]; var != NULL; var = var->next) {
		if (var->key == key)
			return 1;
		num++;
	}
	int ret = 0;
/*
 *
 *
 *
	//struct entry *e = (struct entry*)malloc(sizeof(struct entry));
	struct entry *e = hashmap->entryaddr + hashmap->count * sizeof(struct entry);
    e->key = key;
    e->value = hashmap->valueaddr + hashmap->count * VALUE_SIZE;
	memcpy(e->value, value, strlen(value));
    e->next = buckets->bucket[h];
    buckets->bucket[h] = e;

    hashmap->count++;
    num++;
*
*
*/


/*
     if(hashmap->count == VALUE_COUNT-1){
		printf("malloc\n");
         hashmap->entryaddr = malloc(VALUE_COUNT * sizeof(struct entry));
         hashmap->valueaddr = malloc(VALUE_COUNT * VALUE_SIZE);
     }
*/


	struct entry *e = hashmap->entryaddr + hashmap->count * sizeof(struct entry);
	//struct entry *e = (struct entry*)malloc(sizeof(struct entry));
	ve->key = key;
	ve->next = buckets->bucket[h];
	//ve->value = malloc(VALUE_SIZE);
	ve->value = hashmap->valueaddr + hashmap->count * VALUE_SIZE;



	memcpy(e, ve, sizeof(struct entry));	
	memcpy(e->value, value, strlen(value));

//	memcpy(&(buckets->bucket[h]), e, sizeof(struct entry));
	buckets->bucket[h] = e;
	hashmap->count++;
	num++;

	if (ret)
		return ret;
/*
	if (num > MAX_HASHSET_THRESHOLD ||
			(num > MIN_HASHSET_THRESHOLD &&
			hashmap->count > 2 * buckets->nbuckets)){
//		printf("rebuild at %d \n", buckets->nbuckets);
		hm_v_rebuild(hashmap, buckets->nbuckets * 2);
	}
*/
	return 0;
}

char *hm_v_remove(struct hashmap_tx *hashmap, uint64_t key)
{
	struct buckets *buckets = hashmap->buckets;
	struct entry *var, *prev = NULL;

	uint64_t h = hash(hashmap, buckets, key);
	for (var = buckets->bucket[h];
            var != NULL;
			prev = var, var = var->next) {
		if (var->key == key)
			break;
	}

	if (var == NULL)
		return NULL;
	int ret = 0;

	char *value = var->value;
    if (prev == NULL)
        buckets->bucket[h] = var->next;
    else
        prev->next = var->next;
    hashmap->count--;
    free(var);

	if (ret)
		return NULL;

    /*
	if (hashmap->count < buckets->nbuckets)
		hm_v_rebuild(hashmap, buckets->nbuckets / 2);
    */

	return value;
}

int
hm_v_foreach(struct hashmap_tx *hashmap,
	int (*cb)(uint64_t key, char *value, void *arg), void *arg)
{
	struct buckets *buckets = hashmap->buckets;
	struct entry *var;

	int ret = 0;
	for (size_t i = 0; i < buckets->nbuckets; ++i) {
		if (buckets->bucket[i] == NULL)
			continue;

		for (var = buckets->bucket[i]; var != NULL;
				var = var->next) {
			ret = cb(var->key, var->value, arg);
			if (ret)
				break;
		}
	}

	return ret;
}

static void
hm_v_debug(struct hashmap_tx *hashmap, FILE *out)
{
	struct buckets *buckets = hashmap->buckets;
	struct entry *var;

	fprintf(out, "a: %u b: %u p: %" PRIu64 "\n", hashmap->hash_fun_a,
		hashmap->hash_fun_b, hashmap->hash_fun_p);
	fprintf(out, "count: %" PRIu64 ", buckets: %zu\n",
		hashmap->count, buckets->nbuckets);

	for (size_t i = 0; i < buckets->nbuckets; ++i) {
		if (buckets->bucket[i] == NULL)
			continue;

		int num = 0;
		fprintf(out, "%zu: ", i);
		for (var = buckets->bucket[i]; var != NULL;
				var = var->next) {
			fprintf(out, "%" PRIu64 " ", var->key);
			num++;
		}
		fprintf(out, "(%d)\n", num);
	}
}

char *hm_v_get(struct hashmap_tx *hashmap, uint64_t key)
{
	struct buckets *buckets = hashmap->buckets;
	struct entry *var;

	uint64_t h = hash(hashmap, buckets, key);

	for (var = buckets->bucket[h];
			var != NULL;
			var = var->next)
		if (var->key == key)
			return var->value;

	return NULL;
}

int
hm_v_lookup(struct hashmap_tx *hashmap, uint64_t key)
{
	struct buckets *buckets = hashmap->buckets;
	struct entry *var;

	uint64_t h = hash(hashmap, buckets, key);

	for (var = buckets->bucket[h];
            var != NULL;
			var = var->next)
		if (var->key == key)
			return 1;

	return 0;
}

size_t
hm_v_count(struct hashmap_tx *hashmap)
{
	return hashmap->count;
}

int
hm_v_init(struct hashmap_tx *hashmap)
{
	srand(hashmap->seed);
	return 0;
}

int
hm_v_create(struct hashmap_tx **map, void *arg)
{
	struct hashmap_args *args = (struct hashmap_args *)arg;
	int ret = 0;

    *map = (struct hashmap_tx*)malloc(sizeof(struct hashmap_tx));
    memset(*map, 0, sizeof(struct hashmap_tx));

    uint32_t seed = args ? args->seed : 0;
    create_hashmap(*map, seed);

	return ret;
}

int
hm_v_check(struct hashmap_tx *hashmap)
{
	return true;
}

int
hm_v_cmd(struct hashmap_tx *hashmap,
		unsigned cmd, uint64_t arg)
{
	switch (cmd) {
		case HASHMAP_CMD_REBUILD:
			hm_v_rebuild(hashmap, arg);
			return 0;
		case HASHMAP_CMD_DEBUG:
			if (!arg)
				return -EINVAL;
			hm_v_debug(hashmap, (FILE *)arg);
			return 0;
		default:
			return -EINVAL;
	}
}
