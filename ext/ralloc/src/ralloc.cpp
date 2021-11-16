/*
 * Copyright (C) 2019 University of Rochester. All rights reserved.
 * Licenced under the MIT licence. See LICENSE file in the project root for
 * details. 
 */

#include "ralloc.hpp"

#include <string>
#include <functional>
#include <atomic>
#include <vector>
#include <algorithm>
#include <cstring>

#include "RegionManager.hpp"
#include "BaseMeta.hpp"
#include "SizeClass.hpp"
#include "pm_config.hpp"

using namespace std;

namespace ralloc{
    bool initialized = false;
    /* persistent metadata and their layout */
    BaseMeta* base_md;
    Regions* _rgs;
    std::function<void(const CrossPtr<char, SB_IDX>&, GarbageCollection&)> roots_filter_func[MAX_ROOTS];
    extern SizeClass sizeclass;
};
using namespace ralloc;
extern void public_flush_cache();

int _RP_init(const char* _id, uint64_t size){
    string filepath;
    string id(_id);
    // thread_num = thd_num;

    // reinitialize global variables in case they haven't
    new (&sizeclass) SizeClass();

    filepath = HEAPFILE_PREFIX + id;
    assert(sizeof(Descriptor) == DESCSIZE); // check desc size
    assert(size < MAX_SB_REGION_SIZE && size >= MIN_SB_REGION_SIZE); // ensure user input is >=MAX_SB_REGION_SIZE
    uint64_t num_sb = size/SBSIZE;
    bool restart = Regions::exists_test(filepath+"_basemd");
    _rgs = new Regions();
    for(int i=0; i<LAST_IDX;i++){
    switch(i){
    case DESC_IDX:
        _rgs->create(filepath+"_desc", num_sb*DESCSIZE, true, true);
        break;
    case SB_IDX:
        _rgs->create(filepath+"_sb", num_sb*SBSIZE, true, false);
        break;
    case META_IDX:
        base_md = _rgs->create_for<BaseMeta>(filepath+"_basemd", sizeof(BaseMeta), true);
        break;
    } // switch
    }
    initialized = true;
    return (int)restart;
}

struct RallocHolder{
    int init_ret_val;
    RallocHolder(const char* _id, uint64_t size){
        init_ret_val = _RP_init(_id,size);
    }
    ~RallocHolder(){
        // #ifndef MEM_CONSUME_TEST
        // flush_region would affect the memory consumption result (rss) and 
        // thus is disabled for benchmark testing. To enable, simply comment out
        // -DMEM_CONSUME_TEST flag in Makefile.
        _rgs->flush_region(DESC_IDX);
        _rgs->flush_region(SB_IDX);
        // #endif
        base_md->writeback();
        initialized = false;
        delete _rgs;
    }
};

/* 
 * mmap the existing heap file corresponding to id. aka restart,
 * 		and if multiple heaps exist, print out and let user select;
 * if such a heap doesn't exist, create one. aka start.
 * id is the distinguishable identity of applications.
 */
static RallocHolder* _holder;
int RP_init(const char* _id, uint64_t size){
    _holder = new RallocHolder(_id, size);
    return _holder->init_ret_val;
}

int RP_recover(){
    return (int) base_md->restart();
}

// @seungminjeon: 사용자가 RP_close로 persistent heap을 닫을 수 있도록 함 (기존의 Ralloc 구현은 한 번 열면 프로그램 끝날때까지 닫지 못함)
void RP_close(){
    delete _holder;
}

void* RP_malloc(size_t sz){
    assert(initialized&&"RPMalloc isn't initialized!");
    return base_md->do_malloc(sz);
}

void RP_free(void* ptr){
    assert(initialized&&"RPMalloc isn't initialized!");
    base_md->do_free(ptr);
}

void* RP_set_root(void* ptr, uint64_t i){
    if(ralloc::initialized==false){
        RP_init("no_explicit_init");
    }
    return base_md->set_root(ptr,i);
}
void* RP_get_root_c(uint64_t i){
    assert(initialized);
    return (void*)base_md->get_root<char>(i);
}

// return the size of ptr in byte.
// No check for whether ptr is allocated or isn't null
size_t RP_malloc_size(void* ptr){
    const Descriptor* desc = base_md->desc_lookup(ptr);
    return (size_t)desc->block_size;
}

void* RP_realloc(void* ptr, size_t new_size){
    if(ptr == nullptr) return RP_malloc(new_size);
    if(!_rgs->in_range(SB_IDX, ptr)) return nullptr;
    size_t old_size = RP_malloc_size(ptr);
    if(old_size == new_size) {
        return ptr;
    }
    void* new_ptr = RP_malloc(new_size);
    if(UNLIKELY(new_ptr == nullptr)) return nullptr;
    memcpy(new_ptr, ptr, old_size);
    FLUSH(new_ptr);
    FLUSHFENCE;
    RP_free(ptr);
    return new_ptr;
}

void* RP_calloc(size_t num, size_t size){
    void* ptr = RP_malloc(num*size);
    if(UNLIKELY(ptr == nullptr)) return nullptr;
    size_t real_size = RP_malloc_size(ptr);
    memset(ptr, 0, real_size);
    FLUSH(ptr);
    FLUSHFENCE;
    return ptr;
}

int RP_in_prange(void* ptr){
    if(_rgs->in_range(SB_IDX,ptr)) return 1;
    else return 0;
}

int RP_region_range(int idx, void** start_addr, void** end_addr){
    if(start_addr == nullptr || end_addr == nullptr || idx>=_rgs->cur_idx){
        return 1;
    }
    *start_addr = (void*)_rgs->regions_address[idx];
    *end_addr = (void*) ((uint64_t)_rgs->regions_address[idx] + _rgs->regions[idx]->FILESIZE);
    return 0;
}

void RP_set_root_filter(void (*filter_func)(char*, GarbageCollection&), uint64_t i) {
    assert(i<MAX_ROOTS);
    ralloc::roots_filter_func[i] = filter_func;
}
