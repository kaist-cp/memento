#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <iterator>
#include <thread>
#include <vector>
#include <cstdio>

#include "../libpmemobj_cpp_examples_common.hpp"
#include <libpmemobj++/experimental/clevel_hash.hpp>
#include "../../../common/hash_api.h"
#define LAYOUT "clevel_hash"
// #define KEY_LEN 15
// #define VALUE_LEN 16
// #define HASH_POWER 16
namespace nvobj = pmem::obj;

typedef nvobj::experimental::clevel_hash<nvobj::p<uint64_t>, nvobj::p<uint64_t>>
	persistent_map_type;

struct root
{
	nvobj::persistent_ptr<persistent_map_type> cons;
};
uint64_t coo = 0;
uint64_t ccc = 0;
class clevel : public hash_api
{
	nvobj::pool<root> pop;
	nvobj::persistent_ptr<persistent_map_type> map;

public:
	clevel(int tnum = 1)
	{
		const char *path = PMEM_LOC;
		const size_t pool_size = 64UL * 1024 * 1024 * 1024;
		pop = nvobj::pool<root>::create(
			path, LAYOUT, pool_size, S_IWUSR | S_IRUSR);
		auto proot = pop.root();
		nvobj::transaction::manual tx(pop);
		proot->cons = nvobj::make_persistent<persistent_map_type>();
		proot->cons->set_thread_num(tnum);
		map = proot->cons;
		nvobj::transaction::commit();
	}
	hash_Utilization utilization()
	{
		hash_Utilization h;
		h.load_factor = (float)coo / ccc;
		return h;
	}
	std::string hash_name() { return "clevel"; };
	bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
	{
		pmem::obj::p<uint64_t> k = *reinterpret_cast<const uint64_t *>(key);
		auto r = map->search(persistent_map_type::key_type(k));
		return r.found;
	}
	bool insert(const char *key, size_t key_sz, const char *value,
				size_t value_sz, unsigned tid, unsigned t)
	{
		auto k = *reinterpret_cast<const uint64_t *>(key);
		auto v = *reinterpret_cast<const uint64_t *>(value);
		auto r = map->insert(persistent_map_type::value_type(k, v), tid, t);
		if (!r.found)
		{
			coo++;

			ccc = r.capacity;
		}
		return r.found;
	}
	bool insertResize(const char *key, size_t key_sz, const char *value,
					  size_t value_sz, unsigned tid, unsigned t)
	{
		auto k = *reinterpret_cast<const uint64_t *>(key);
		auto v = *reinterpret_cast<const uint64_t *>(value);
		auto r = map->insert(persistent_map_type::value_type(k, v), tid, t);
		return r.expanded;
	}
	bool update(const char *key, size_t key_sz, const char *value,
				size_t value_sz)
	{
		return true;
	}

	bool remove(const char *key, size_t key_sz, unsigned tid)
	{
		auto k = *reinterpret_cast<const uint64_t *>(key);
		auto r = map->erase(persistent_map_type::key_type(k), tid);
		return true;
	}

	int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
	{
		return scan_sz;
	}
};
extern "C" hash_api *create_tree(const tree_options_t &opt, unsigned sz = 1024, unsigned tnum = 1)
{
	clevel *c = new clevel(tnum);
	return c;
}
