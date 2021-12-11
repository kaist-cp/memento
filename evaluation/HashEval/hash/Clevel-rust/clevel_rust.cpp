#ifndef CLEVEL_RUST
#define CLEVEL_RUST

#include <hash_api.h>
#include <vmem.h>
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <iostream>

#include "pair.h"
#include "persist.h"

#ifndef _EXAMPLE_H
#define _EXAMPLE_H

// Same key/value type as Clevel
typedef uint64_t Key;
typedef uint64_t Value;

const uint IX_OBJ = 0;
const uint IX_NR_MEMENTO = 1;
const uint IX_MEMENTO_START = 2;

#ifdef __cplusplus
extern "C"
{
#endif
    typedef struct _poolhandle PoolHandle;
    typedef struct _clevel ClevelRust;
    typedef struct _memento ClevelMemento;

    PoolHandle *pool_create(char *path, size_t size, int tnum);
    void *get_root(size_t ix, PoolHandle *pool);
    bool run_search(ClevelMemento *m, ClevelRust *obj, Key k, PoolHandle *pool);
    bool run_insert(ClevelMemento *m, ClevelRust *obj, Key k, Value v, PoolHandle *pool);
    bool run_update(ClevelMemento *m, ClevelRust *obj, Key k, Value v, PoolHandle *pool);
    bool run_delete(ClevelMemento *m, ClevelRust *obj, Key k, PoolHandle *pool);

#ifdef __cplusplus
}
#endif
#endif

using namespace std;

class CLevelMemento : public hash_api
{
    PoolHandle *pool;
    ClevelRust *c;
    ClevelMemento **m; // tnum memento*`

public:
    CLevelMemento(int tnum = 1)
    {
        char *path = "/mnt/pmem0/clevel_memento";
        const size_t size = 64UL * 1024 * 1024 * 1024;
        pool = pool_create(path, size, tnum);
        c = reinterpret_cast<ClevelRust *>(get_root(IX_OBJ, pool));
        m = (ClevelMemento **)malloc(sizeof(ClevelMemento *) * tnum);
        for (int tid = 0; tid < tnum; ++tid)
        {
            m[tid] = reinterpret_cast<ClevelMemento *>(get_root(IX_MEMENTO_START + tid, pool));
        }
    }
    ~CLevelMemento(){};
    std::string hash_name()
    {
        return "clevel-memento";
    };
    bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        return run_search(m[tid], c, k, pool);
    }

    bool insert(const char *key, size_t key_sz, const char *value,
                size_t value_sz, unsigned tid, unsigned t)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        auto v = *reinterpret_cast<const Value *>(value);
        return run_insert(m[tid], c, k, v, pool);
    }
    bool insertResize(const char *key, size_t key_sz, const char *value,
                      size_t value_sz, unsigned tid, unsigned t)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        auto v = *reinterpret_cast<const Value *>(value);
        return run_insert(m[tid], c, k, v, pool);
    }
    bool update(const char *key, size_t key_sz, const char *value,
                size_t value_sz)
    {
        // return true same as clevel c++.
        return true;
    }

    bool remove(const char *key, size_t key_sz, unsigned tid)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        return run_delete(m[tid], c, k, pool);
    }

    int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
    {
        // return scan_sz same as clevel c++.
        return scan_sz;
    }
};

extern "C" hash_api *create_tree(const tree_options_t &opt, unsigned sz,
                                 unsigned tnum)
{
    CLevelMemento *c = new CLevelMemento(tnum);
    return c;
}

#endif // EXTENDIBLE_PTR_H_
