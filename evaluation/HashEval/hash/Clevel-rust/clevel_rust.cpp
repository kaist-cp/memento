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

// Same key/value type as Clevel
typedef uint64_t Key;
typedef uint64_t Value;

enum RootIdx
{
    RootObj,       // root obj
    CASCheckpoint, // cas general checkpoint
    NrMemento,     // memento의 개수
    MementoStart,  // root memento(s) 시작 위치
};

#ifdef __cplusplus
extern "C"
{
#endif
    typedef struct _poolhandle PoolHandle;
    PoolHandle *pool_create(char *path, size_t size, int tnum);
    void *get_root(size_t ix, PoolHandle *pool);
    void thread_init(int tid);

    typedef struct _clevel Clevel;
    bool search(Clevel *obj, unsigned tid, Key k, PoolHandle *pool);
    size_t get_capacity(Clevel *c, PoolHandle *pool);
    bool is_resizing(Clevel *c, PoolHandle *pool);

    typedef struct _memento ClevelMemento;
    bool run_insert(ClevelMemento *m, Clevel *obj, unsigned tid, Key k, Value v, PoolHandle *pool);
    bool run_update(ClevelMemento *m, Clevel *obj, unsigned tid, Key k, Value v, PoolHandle *pool);
    bool run_delete(ClevelMemento *m, Clevel *obj, unsigned tid, Key k, PoolHandle *pool);
    void run_resize_loop(ClevelMemento *m, Clevel *obj, unsigned tid, PoolHandle *pool);

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
        const size_t size = 128UL * 1024 * 1024 * 1024;
        pool = pool_create(path, size, tnum);
        c = reinterpret_cast<Clevel *>(get_root(RootObj, pool));
        m = (ClevelMemento **)malloc(sizeof(ClevelMemento *) * tnum);

        // `0~tnum-1` thread for insert, delete, search
        for (int tid = 0; tid < tnum; ++tid)
        {
            m[tid] = reinterpret_cast<ClevelMemento *>(get_root(MementoStart + tid, pool));
        }

        // `tnum` thread is only for resize loop
        ClevelMemento *m_resize = reinterpret_cast<ClevelMemento *>(get_root(MementoStart + tnum, pool));
        std::thread{run_resize_loop, m_resize, c, tnum, pool}.detach();
    }
    ~CLevelMemento(){
        // TODO: pool close?
    };
    bool hash_is_resizing()
    {
        return is_resizing(c, pool);
    }
    std::string hash_name()
    {
        return "clevel-memento";
    };
    hash_Utilization utilization()
    {
        hash_Utilization h;
        h.load_factor = (float)inserted / get_capacity(c, pool);
        return h;
    }
    void thread_ini(int id)
    {
        thread_init(id);
    }
    bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        return search(c, tid, k, pool);
    }

    bool insert(const char *key, size_t key_sz, const char *value,
                size_t value_sz, unsigned tid, unsigned t)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        auto v = *reinterpret_cast<const Value *>(value);

        bool ret = run_insert(m[tid], c, tid, k, v, pool);
        if (ret)
        {
            inserted += 1;
        }
        return ret;
    }
    bool insertResize(const char *key, size_t key_sz, const char *value,
                      size_t value_sz, unsigned tid, unsigned t)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        auto v = *reinterpret_cast<const Value *>(value);
        return run_insert(m[tid], c, tid, k, v, pool);
    }
    bool update(const char *key, size_t key_sz, const char *value,
                size_t value_sz)
    {
        // return true same as clevel c++. (TODO: 한다면 tid가 문제. hash API는 tid 안받지만 우리는 필요?)
        return true;
    }

    bool remove(const char *key, size_t key_sz, unsigned tid)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        return run_delete(m[tid], c, tid, k, pool);
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
