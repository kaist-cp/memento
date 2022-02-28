#include "SOFT.h"

#include <iostream>
extern "C" hash_api *create_tree(const tree_options_t &opt, unsigned sz, unsigned tnum)
{
  return new SOFTHashTable<uintptr_t>();
}