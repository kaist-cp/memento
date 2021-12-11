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

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct _custom_clevel ClevelRust;

    ClevelRust *clevel_new(int tnum);
    void clevel_free(ClevelRust *o);
    bool clevel_search(ClevelRust *o, Key k, unsigned tid);
    bool clevel_insert(ClevelRust *o, Key k, Value v, unsigned tid);
    bool clevel_update(ClevelRust *o, Key k, Value v, unsigned tid);
    bool clevel_delete(ClevelRust *o, Key k, unsigned tid);
    size_t clevel_get_capacity(ClevelRust *c);

#ifdef __cplusplus
}
#endif
#endif

using namespace std;

uint64_t inserted = 0;
class CLevelMemento : public hash_api
{
    ClevelRust *c;

public:
    CLevelMemento(int tnum = 1)
    {
        c = clevel_new(tnum);
    }
    ~CLevelMemento(void)
    {
        clevel_free(c);
    };
    std::string hash_name() { return "clevel-memento"; };
    hash_Utilization utilization()
    {
        hash_Utilization h;
        h.load_factor = (float)inserted / clevel_get_capacity(c);
        return h;
    }
    bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        return clevel_search(c, k, tid);
    }

    bool insert(const char *key, size_t key_sz, const char *value,
                size_t value_sz, unsigned tid, unsigned t)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        auto v = *reinterpret_cast<const Value *>(value);
        bool ret = clevel_insert(c, k, v, tid);
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
        return clevel_insert(c, k, v, tid);
    }
    bool update(const char *key, size_t key_sz, const char *value,
                size_t value_sz)
    {
        // clevel c++과 동일하게 return true. 어차피 실험은 insert, delete, search만 함
        return true;
    }

    bool remove(const char *key, size_t key_sz, unsigned tid)
    {
        auto k = *reinterpret_cast<const Key *>(key);
        return clevel_delete(c, k, tid);
    }

    int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
    {
        // clevel c++과 동일하게 return scan_sz
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
