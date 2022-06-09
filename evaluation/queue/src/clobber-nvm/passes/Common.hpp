#pragma once
#include <queue>
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/TypeBuilder.h"
#include "llvm/Support/Debug.h"
#include "llvm/IR/IntrinsicInst.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include "llvm/IR/Dominators.h"
#include "llvm/Analysis/LoopInfo.h"
#include "llvm/Analysis/AliasAnalysis.h"
#include "llvm/IR/Type.h"
#include "llvm/Transforms/Utils/ValueMapper.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm/IR/Instruction.h"
#include "llvm/IR/MDBuilder.h"
#include <sstream>

#ifndef NDEBUG
#define DEBUG(stream) outs() << stream
#define DEBUG_VALUE(caption, valuePtr) {\
    std::string str;\
    raw_string_ostream rso(str);\
    if (valuePtr != NULL) valuePtr->print(rso);\
    else str = "NULL";\
    outs() << caption << ": " << str << "\n";\
}
#else
#define DEBUG(stream) { }
#define DEBUG_VALUE(caption, valuePtr) { }
#endif

using namespace llvm;

namespace llvm{

static std::string getLocator(const Instruction &I) {
  unsigned Offset = 0;
  const BasicBlock *BB = I.getParent();
  int hitI = 0;

  const Instruction *instI = &I;
  for (BasicBlock::const_iterator It = BB->end(); It != BB->begin(); --It){
    if(hitI == 1){
        ++Offset;
    }
    if(hitI == 0){
        const Instruction *tmp_inst = &(*It);
        if(instI == tmp_inst)
            hitI = 1;
    }
  }

  /*
  for (BasicBlock::const_iterator It = I; It != BB->begin(); --It){
    ++Offset;
  }
  */

  std::stringstream SS;
  SS << BB->getName().str() << ":" << Offset;
  return SS.str();
}


template <typename T1, typename T2>
struct tier {
  typedef T1 &first_type;
  typedef T2 &second_type;

  first_type first;
  second_type second;

  tier(first_type f, second_type s) : first(f), second(s) { }
  tier& operator=(const std::pair<T1, T2>& p) {
    first = p.first;
    second = p.second;
    return *this;
  }
};

template <typename T1, typename T2>
inline tier<T1, T2> tie(T1& f, T2& s) {
  return tier<T1, T2>(f, s);
}



}


typedef iplist<BasicBlock>::iterator BBIterator;
typedef iplist<Instruction>::iterator InstIterator;
