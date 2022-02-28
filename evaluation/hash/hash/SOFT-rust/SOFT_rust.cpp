#ifndef SOFT_RUST
#define SOFT_RUST

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
#include <thread>

#ifndef _EXAMPLE_H
#define _EXAMPLE_H

// Same key/value type as SOFT
typedef uint64_t Key;
typedef uint64_t Value;

enum RootIdx
{
    RootObj,       // root obj
    CASCheckpoint, // cas general checkpoint
    NrMemento,     // number of root mementos
    MementoStart,  // start index of root memento(s)
};

#ifdef __cplusplus
extern "C"
{
#endif
    typedef struct _poolhandle PoolHandle;
    PoolHandle *pool_create(char *path, size_t size, int tnum);
    void *get_root(size_t ix, PoolHandle *pool);
    void thread_init(int tid, PoolHandle *pool);

    typedef struct _SOFT SOFT;
    bool search(SOFT *obj, unsigned tid, Key k, PoolHandle *pool);

    typedef struct _memento SOFTMemento;
    bool run_insert(SOFTMemento *m, SOFT *obj, unsigned tid, Key k, Value v, PoolHandle *pool);
    bool run_delete(SOFTMemento *m, SOFT *obj, unsigned tid, Key k, PoolHandle *pool);

#ifdef __cplusplus
}
#endif
#endif

using namespace std;
uint64_t inserted = 0;
class SOFTRust : public hash_api
{
    PoolHandle *pool;
    SOFT *c;
    SOFTMemento **m; // tnum memento*`

public:
    SOFTRust(int tnum = 1)
    {
        char *path = "/mnt/pmem0/SOFT_memento";
        const size_t size = 128UL * 1024 * 1024 * 1024;
        pool = pool_create(path, size, tnum);
        c = reinterpret_cast<SOFT *>(get_root(RootObj, pool));
        m = (SOFTMemento **)malloc(sizeof(SOFTMemento *) * tnum);
        for (int tid = 1; tid <= tnum; ++tid)
        {
            m[tid] = reinterpret_cast<SOFTMemento *>(get_root(MementoStart + tid, pool));
        }
        thread_ini(-1);
    }
    ~SOFTRust(){
        // TODO: pool close?
    };
    bool hash_is_resizing()
    {
        return false;
    }
    std::string hash_name()
    {
        return "SOFT-memento";
    };
    hash_Utilization utilization()
    {
        hash_Utilization h;
        return h;
    }
    void thread_ini(int tid)
    {
        tid = tid + 1; // pibench can give tid 0, but tid in memento starts from 1
        thread_init(tid, pool);
    }
    bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
    {
        tid = tid + 1;
        auto k = *reinterpret_cast<const Key *>(key);
        return search(c, tid, k, pool);
    }

    bool insert(const char *key, size_t key_sz, const char *value,
                size_t value_sz, unsigned tid, unsigned t)
    {
        tid = tid + 1;
        auto k = *reinterpret_cast<const Key *>(key);
        auto v = *reinterpret_cast<const Value *>(value);

        return run_insert(m[tid], c, tid, k, v, pool);
    }
    bool update(const char *key, size_t key_sz, const char *value,
                size_t value_sz)
    {
        return true;
    }

    bool remove(const char *key, size_t key_sz, unsigned tid)
    {
        tid = tid + 1;
        auto k = *reinterpret_cast<const Key *>(key);
        return run_delete(m[tid], c, tid, k, pool);
    }

    int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
    {
        return scan_sz;
    }
};

extern "C" hash_api *create_tree(const tree_options_t &opt, unsigned sz,
                                 unsigned tnum)
{
    SOFTRust *c = new SOFTRust(tnum);
    return c;
}

#endif // EXTENDIBLE_PTR_H_
