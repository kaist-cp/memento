//===-------- MemoryIdempotenceAnalysis.h -----------------------*- C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// This file contains the interface for querying (no updating) the idempotent
// region information at the LLVM IR level in terms of the "cuts" that define
// them.  See "Static Analysis and Compiler Design for Idempotent Processing" in
// PLDI '12.
//
// This interface is greatly simplified by the use of the pimpl idiom (*Impl)
// which hides the implementation details.
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_MEMORYIDEMPOTENCEANALYSIS_H
#define LLVM_MEMORYIDEMPOTENCEANALYSIS_H

#include "llvm/IR/Function.h"
#include "llvm/IR/Instruction.h"
#include "llvm/IR/Module.h"
#include "llvm/Pass.h"
#include "llvm/ADT/SmallPtrSet.h"

namespace llvm {

class MemoryIdempotenceAnalysisImpl;
class MemoryIdempotenceAnalysis : public FunctionPass {
  typedef std::pair<Instruction *, Instruction *> AntidependencePairTy;
  typedef SmallPtrSet<Instruction *, 16> CutSet;
  typedef SmallVector<AntidependencePairTy, 16> AntidependencePairs;

 public:
  static char ID;
  MemoryIdempotenceAnalysis() : FunctionPass(ID) {}

  virtual void getAnalysisUsage(AnalysisUsage &AU) const;
  virtual void releaseMemory();
  virtual void print(raw_ostream &OS, const Module *M = 0) const;

  virtual bool doInitialization(Module &M);
  virtual bool runOnFunction(Function &F);
  virtual bool doFinalization(Module &M);

  // Iteration support (const only).
  //typedef CutSet::const_iterator const_iterator;
  typedef AntidependencePairs::const_iterator const_iterator;
  //const_iterator begin()  const { return CutSet_->begin(); }
  const_iterator begin() const{return AntidependencePairs_->begin();}
  //const_iterator end()    const { return CutSet_->end(); }
  const_iterator end() const{return AntidependencePairs_->end();}
  //bool           empty()  const { return CutSet_->empty(); }
  bool empty() const{return AntidependencePairs_->empty();}
 private:
  friend class MemoryIdempotenceAnalysisImpl;
  friend class NaiveUndo;
  // Our key output data structure.
  CutSet *CutSet_;
  AntidependencePairs *AntidependencePairs_;

  // Hide implementation details.
  MemoryIdempotenceAnalysisImpl *Impl;
};

} // End llvm namespace

#endif
