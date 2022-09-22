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
#include <thread>

#ifndef _EXAMPLE_H
#define _EXAMPLE_H

// Same key,value type as Clevel
typedef uint64_t Key;
typedef uint64_t Value;

enum RootIdx
{
    RootObj,        // root obj
    CASHelpArr,     // cas help array
    CASHelpDescArr, // cas help descriptor array
    NrMemento,      // number of root mementos
    MementoStart,   // start index of root memento(s)
};

#ifdef __cplusplus
extern "C"
{
#endif
    typedef struct _poolhandle PoolHandle;
    PoolHandle *pool_create(char *path, size_t size, int tnum);
    void *get_root(size_t ix, PoolHandle *pool);
    void thread_init(int tid, PoolHandle *pool);

    typedef struct _clevel Clevel;
    bool search(Clevel *obj, unsigned tid, Key k);
    size_t get_capacity(Clevel *c, unsigned tid);
    bool is_resizing(Clevel *c, unsigned tid);

    typedef struct _memento ClevelMemento;
    bool run_insert(ClevelMemento *m, Clevel *obj, unsigned tid, Key k, Value v);
    bool run_delete(ClevelMemento *m, Clevel *obj, unsigned tid, Key k);
    void run_resize(ClevelMemento *m, Clevel *obj, unsigned tid);

#ifdef __cplusplus
}
#endif
#endif

using namespace std;
uint64_t inserted = 0;
class CLevelMemento : public hash_api
{
    PoolHandle *pool;
    Clevel *c;
    ClevelMemento **m; // tnum memento*`

public:
    CLevelMemento(int tnum = 1)
    {
        char *path = "/mnt/pmem0/clevel_memento";
        const size_t size = 256UL * 1024 * 1024 * 1024;
        pool = pool_create(path, size, tnum);
        c = reinterpret_cast<Clevel *>(get_root(RootObj, pool));
        m = (ClevelMemento **)malloc(sizeof(ClevelMemento *) * tnum);

        // `1~tnum` thread for insert, delete, search
        for (int tid = 1; tid <= tnum; ++tid)
        {
            m[tid] = reinterpret_cast<ClevelMemento *>(get_root(MementoStart + tid, pool));
        }

        // `tnum+1` thread is only for resize loop
        int tid_resize = tnum + 1;
        ClevelMemento *m_resize = reinterpret_cast<ClevelMemento *>(get_root(MementoStart + tid_resize, pool));
        thread_init(tid_resize, pool);
        std::thread{run_resize, m_resize, c, tid_resize}.detach();
    }
    ~CLevelMemento(){};
    bool hash_is_resizing()
    {
        return is_resizing(c, 1);
    }
    std::string hash_name()
    {
        return "clevel-memento";
    };
    hash_Utilization utilization()
    {
        hash_Utilization h;
        h.load_factor = ((float)inserted / get_capacity(c, 1)) * 100;
        return h;
    }
    void thread_ini(int tid)
    {
        tid = tid + 1; // pibench can give tid 0, but tid in memento starts from 1
        thread_init(tid, pool);
    }
    bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
    {
        tid = tid + 1; // pibench can give tid 0, but tid in memento starts from 1
        auto k = *reinterpret_cast<const Key *>(key);
        return search(c, tid, k);
    }

    bool insert(const char *key, size_t key_sz, const char *value,
                size_t value_sz, unsigned tid, unsigned t)
    {
        tid = tid + 1; // pibench can give tid 0, but tid in memento starts from 1
        auto k = *reinterpret_cast<const Key *>(key);
        auto v = *reinterpret_cast<const Value *>(value);

        bool ret = run_insert(m[tid], c, tid, k, v);
        if (ret)
        {
            inserted += 1;
        }
        return ret;
    }
    bool insertResize(const char *key, size_t key_sz, const char *value,
                      size_t value_sz, unsigned tid, unsigned t)
    {
        tid = tid + 1; // pibench can give tid 0, but tid in memento starts from 1
        auto k = *reinterpret_cast<const Key *>(key);
        auto v = *reinterpret_cast<const Value *>(value);
        return run_insert(m[tid], c, tid, k, v);
    }
    bool update(const char *key, size_t key_sz, const char *value,
                size_t value_sz)
    {
        return true;
    }

    bool remove(const char *key, size_t key_sz, unsigned tid)
    {
        tid = tid + 1; // pibench can give tid 0, but tid in memento starts from 1
        auto k = *reinterpret_cast<const Key *>(key);
        return run_delete(m[tid], c, tid, k);
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
