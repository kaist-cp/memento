//===----------------------------------------------------------------------===//
//
// This file contains the implementation for computing clobber writes at the LLVM IR level.
// The implementation is based on the code provided in
// "Static Analysis and Compiler Design for Idempotent Processing" in PLDI
// '12.
// See "Clobber-NVM: Log Less, Re-execute More" in ASPLOS'21
//
//
//===----------------------------------------------------------------------===//


#define DEBUG_TYPE "memory-idempotence-analysis"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Instruction.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/Module.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/Analysis/AliasAnalysis.h"
#include "llvm/Analysis/CaptureTracking.h"
#include "llvm/IR/Dominators.h"
#include "llvm/Analysis/LoopInfo.h"
#include "llvm/Analysis/MemoryBuiltins.h"
#include "MemoryIdempotenceAnalysis.h"
#include "llvm/Support/CommandLine.h"
#include "llvm/Support/Debug.h"
#include "llvm/IR/PredIteratorCache.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include <algorithm>
#include <sstream>
#include <vector>
#include "Common.hpp"

using namespace llvm;


//===----------------------------------------------------------------------===//
// Helpers
//===----------------------------------------------------------------------===//

static bool isSubloopPreheader(const BasicBlock &BB,
                               const LoopInfo &LI) {
  Loop *L = LI.getLoopFor(&BB);
  if (L)
    for (Loop::iterator I = L->begin(), E = L->end(); I != E; ++I)
      if (&BB == (*I)->getLoopPreheader())
        return true;
  return false;
}


namespace {
  typedef std::pair<Instruction *, Instruction *> AntidependencePairTy;
  typedef SmallVector<Instruction *, 16> AntidependencePathTy;
}

namespace llvm{
  static raw_ostream &operator<<(raw_ostream &OS, const AntidependencePairTy &P);
  static raw_ostream &operator<<(raw_ostream &OS, const AntidependencePathTy &P);
}
raw_ostream &llvm::operator<<(raw_ostream &OS, const AntidependencePairTy &P) {
  OS << "Antidependence Pair (" << getLocator(*P.first) << ", " 
    << getLocator(*P.second) << ")";
  return OS;
}

raw_ostream &llvm::operator<<(raw_ostream &OS, const AntidependencePathTy &P) {
  OS << "[";
  for (AntidependencePathTy::const_iterator I = P.begin(), First = I,
       E = P.end(); I != E; ++I) {
    if (I != First)
      OS << ", ";
    OS << getLocator(**I);
  }
  OS << "]";
  return OS;
}

//===----------------------------------------------------------------------===//
// MemoryIdempotenceAnalysisImpl
//===----------------------------------------------------------------------===//

class llvm::MemoryIdempotenceAnalysisImpl {
 private:
  // Constructor.
  MemoryIdempotenceAnalysisImpl(MemoryIdempotenceAnalysis *MIA) : MIA_(MIA) {}

  // Forwarded function implementations.
  void releaseMemory();
  void print(raw_ostream &OS, const Module *M = 0) const;
  bool runOnFunction(Function &F);

 private:
  friend class MemoryIdempotenceAnalysis;
  friend class NaiveUndo;
  MemoryIdempotenceAnalysis *MIA_;

  // Final output structure.
  MemoryIdempotenceAnalysis::CutSet CutSet_;

  // Intermediary data structure 1.
  typedef SmallVector<AntidependencePairTy, 16> AntidependencePairs;
  MemoryIdempotenceAnalysis::AntidependencePairs AntidependencePairs_;

  // Intermediary data structure 2.
  typedef SmallVector<AntidependencePathTy, 16> AntidependencePaths;
  AntidependencePaths AntidependencePaths_;

  // Other things we use.
  PredIteratorCache PredCache_;
  Function *F_;
  AliasAnalysis *AA_;
  DominatorTree *DT_;
  LoopInfo *LI_;

  // Helper functions.
  void forceCut(BasicBlock::iterator I);
  void findAntidependencePairs(StoreInst *Store);
  bool scanForAliasingLoad(BasicBlock::iterator I,
                           BasicBlock::iterator E,
                           StoreInst *Store,
                           Value *Pointer,
                           unsigned PointerSize);
  void computeAntidependencePaths();
  bool preceedAntidependence(AntidependencePairs::iterator I);
};

void MemoryIdempotenceAnalysisImpl::releaseMemory() {
  CutSet_.clear();
  AntidependencePairs_.clear();
  AntidependencePaths_.clear();
  PredCache_.clear();
}

static bool forcesCut(const Instruction &I) {
  // See comment at the head of forceCut() further below.
  //if (const LoadInst *L = cast<LoadInst>(&I)){
  if (isa<LoadInst>(&I)){
    const LoadInst *L = dyn_cast<LoadInst>(&I);
    return L->isVolatile();
  }
  //if (const StoreInst *S = cast<StoreInst>(&I)){
  if (isa<StoreInst>(&I)){
    const StoreInst *S = dyn_cast<StoreInst>(&I);
    return S->isVolatile();
  }
  return (isa<CallInst>(I) ||
          isa<InvokeInst>(I) ||
          isa<VAArgInst>(&I) ||
          isa<FenceInst>(&I) ||
          isa<AtomicCmpXchgInst>(&I) ||
          isa<AtomicRMWInst>(&I));
}

bool MemoryIdempotenceAnalysisImpl::runOnFunction(Function &F) {

  F_  = &F;
  AA_ = &MIA_->getAnalysis<AAResultsWrapperPass>().getAAResults();
  DT_ = &MIA_->getAnalysis<DominatorTreeWrapperPass>().getDomTree();
  LI_ = &MIA_->getAnalysis<LoopInfoWrapperPass>().getLoopInfo();

  for (Function::iterator BB = F.begin(); BB != F.end(); ++BB)
    for (BasicBlock::iterator I = BB->begin(); I != BB->end(); ++I)
	      if (forcesCut(*I))
		forceCut(I);
  int StoreinstNum = 0;
  for (Function::iterator BB = F.begin(); BB != F.end(); ++BB){
    for (BasicBlock::iterator I = BB->begin(); I != BB->end(); ++I){
      if (StoreInst *Store = dyn_cast<StoreInst>(I)){
	findAntidependencePairs(Store);
	StoreinstNum++;
      }
    }
  }


  int ReadOnlyNum = 0;
  int ReadinstNum = 0;
  int ReaddiffinstNum = 0;
  SmallPtrSet<Value*, 32> VisitedLoad;
  for (Function::iterator BB = F.begin(); BB != F.end(); ++BB){
    for (BasicBlock::iterator I = BB->begin(); I != BB->end(); ++I){
      if (LoadInst *Load = dyn_cast<LoadInst>(I)){
        ReadinstNum++;
	Value *Lptr = Load->getPointerOperand();
	if(VisitedLoad.insert(Lptr).second)
	    ReaddiffinstNum++;
      }
    }
  }


  int erasenum = 0;
  for (Function::iterator BB = F.begin(); BB != F.end(); ++BB){
    for (BasicBlock::iterator I = BB->begin(); I != BB->end(); ++I){
      if (StoreInst *Store = dyn_cast<StoreInst>(I)){
	Value *Sptr = Store->getPointerOperand();
	if(VisitedLoad.erase(Sptr))
	  erasenum++;
      }
    }
  }
  if (AntidependencePairs_.empty()){
    return false;
  }
  computeAntidependencePaths();
  return false;
}

void MemoryIdempotenceAnalysisImpl::forceCut(BasicBlock::iterator I) {
  // These cuts actually need to occur at the machine level.  Calls and invokes
  // are one common case that we are handled after instruction selection; see
  // patchCallingConvention() in PatchMachineIdempotentRegions.  In the absence
  // of any actual hardware support, the others are just approximated here.
  if (CallSite(&*I))
    return;

  //DEBUG(dbgs() << " Inserting forced cut at " << getLocator(*I) << "\n");
  CutSet_.insert(&*(++I));
}

void MemoryIdempotenceAnalysisImpl::findAntidependencePairs(StoreInst *Store) {
  Value *Pointer = Store->getOperand(1);
  const DataLayout &DL = Store->getModule()->getDataLayout();
  unsigned PointerSize = DL.getTypeStoreSize(Store->getOperand(0)->getType());

  // Perform a reverse depth-first search to find aliasing loads.
  typedef std::pair<BasicBlock *, BasicBlock::iterator> WorkItem;
  SmallVector<WorkItem, 8> Worklist;
  SmallPtrSet<BasicBlock *, 32> Visited;

  BasicBlock *StoreBB = Store->getParent();
  Worklist.push_back(WorkItem(StoreBB, Store));
  do {
    BasicBlock *BB;
    BasicBlock::iterator I, E;
    tie(BB, I) = Worklist.pop_back_val();

    // If we are revisiting StoreBB, we scan to Store to complete the cycle.
    // Otherwise we end at BB->begin().

    int hitStore = 0;
    /*Added to fix instr to iterator convertion*/
    BasicBlock::iterator StoreIterator;
    for (BasicBlock::iterator ii = StoreBB->begin(); ((ii != StoreBB->end())&&(hitStore==0)); ii++){
      Instruction *inst = &(*ii);
      if(inst == Store){
	StoreIterator = ii;
	hitStore = 1;
      }
    }
    E = (BB == StoreBB && I == BB->end()) ? StoreIterator : BB->begin();

    // Scan for an aliasing load.  Terminate this path if we see one or a cut is
    // already forced.
    if (scanForAliasingLoad(I, E, Store, Pointer, PointerSize))
      continue;

    // If the path didn't terminate, continue on to predecessors.

    /*updated due to BasicBlock **GetPreds(BasicBlock *BB) was changed to 
    private, and insert was changed to pair in LLVM-7.0.0*/
    ArrayRef<BasicBlock *> BlockArray = PredCache_.get(BB);
    for(ArrayRef<BasicBlock*>::iterator BI = BlockArray.begin(); 
	BI!=BlockArray.end(); ++BI){
      BasicBlock *B = (*BI);
    //for (BasicBlock **P = PredCache_.GetPreds(BB); *P; ++P)
      if (Visited.insert(B).second)
        Worklist.push_back(WorkItem((B), (B)->end()));
    }
    /*End of editing*/


  } while (!Worklist.empty());
}

bool MemoryIdempotenceAnalysisImpl::scanForAliasingLoad(BasicBlock::iterator I,
                                                        BasicBlock::iterator E,
                                                        StoreInst *Store,
                                                        Value *Pointer,
                                                        unsigned PointerSize) {
  while (I != E) {
    --I;
    // If we see a forced cut, the path is already cut; don't scan any further.
    if (forcesCut(*I)){
      return true;
    }
    // Otherwise, check for an aliasing load.

    if (LoadInst *Load = dyn_cast<LoadInst>(I)) {
	uint16_t conv1 = static_cast<uint16_t>(AA_->getModRefInfo(Load, MemoryLocation(Pointer)));
	uint16_t conv2 = static_cast<uint16_t>(ModRefInfo::MustRef);
	if (conv1 & conv2) {
        AntidependencePairTy Pair = AntidependencePairTy(dyn_cast<LoadInst>(I), Store);
        AntidependencePairs_.push_back(Pair);
        return true;
      }
    }
  }
  return false;
}

void MemoryIdempotenceAnalysisImpl::computeAntidependencePaths() {
  // Compute an antidependence path for each antidependence pair.
  bool RAWWAR[AntidependencePairs_.size()];
  int i = 0;

  for(i = 0; i< AntidependencePairs_.size(); i++)
	RAWWAR[i] = false;
  i = 0;

  for (AntidependencePairs::iterator I = AntidependencePairs_.begin(),
       E = AntidependencePairs_.end(); I != E; ++I) {
	Instruction *SI, *LI;
        tie(LI, SI) = *I;
  }


  for (AntidependencePairs::iterator I = AntidependencePairs_.begin(), 
       E = AntidependencePairs_.end(); I != E; ++I) {

	if(preceedAntidependence(I)){
		RAWWAR[i] = true;
		Instruction *SI, *LI;
        	tie(LI, SI) = *I;
	}
	else
		RAWWAR[i] = false;

	i++;
  }


  int size = AntidependencePairs_.size();
  AntidependencePairs::iterator I = AntidependencePairs_.begin();
  for(int j=0; j<size; j++){
         Instruction *SI, *LI;
        tie(LI, SI) = *I;
        if(RAWWAR[j]){
		AntidependencePairs::iterator tmpI = I;
		//++I;
		AntidependencePairs_.erase(tmpI);
	}
        else{
		++I;
	}
        //j++;
  }



  for (AntidependencePairs::iterator I = AntidependencePairs_.begin(),
       E = AntidependencePairs_.end(); I != E; ++I) {
	Instruction *SI, *LI;
	tie(LI, SI) = *I;
  }	
}


bool MemoryIdempotenceAnalysisImpl::preceedAntidependence(AntidependencePairs::iterator I){
    BasicBlock::iterator Load, Store;

    Instruction *LI = &*Load;
    Instruction *SI = &*Store;
    tie(LI, SI) = *I;

    // The rest of the path consists of other stores/loads that dominate Store or
    // dominate Load.  Handle the block-local case quickly.
    BasicBlock::iterator Cursor;

   int hitStore = 0;
    /*Added to fix instr to iterator convertion*/
   //BasicBlock::iterator StoreIterator;
   for (BasicBlock::iterator ii = SI->getParent()->begin(); ((ii != SI->getParent()->end())&&(hitStore==0)); ii++){
        Instruction *inst = &(*ii);
        if(inst == SI){
	  Store = ii;
	  Cursor = ii;
          hitStore = 1;
        }
    }

    int hitLoad = 0;
    for (BasicBlock::iterator ii = LI->getParent()->begin(); ((ii != LI->getParent()->end())&&(hitLoad==0)); ii++){
        Instruction *inst = &(*ii);
        if(inst == LI){
          Load = ii;
	  //Cursor = ii;
          hitLoad = 1;
        }
    }

    BasicBlock *SBB = SI->getParent(), *LBB = LI->getParent();
    Instruction *CI = &*Cursor;
    /*the antidependency pair's Store and Load are in the basic block*/
    /*First case: test if there's a store local to load's BB at AA with load*/
    /*start from the load that caused the antidependency, see if it's precedeed by 
    any store that access the same memory location. Iterate until the beginning of BB*/


    //if SI is not the first instruction in the BB
    if(SI->getParent()->begin() != Cursor){

      Cursor--;
   
      for (; Cursor != SI->getParent()->begin(); Cursor--){
        if (isa<StoreInst>(Cursor)){
    	  Value *SIPointer = SI->getOperand(1);
	
	  CI = &*Cursor;
	  Value *CIPointer = CI->getOperand(1);

	  uint16_t s1s2 = static_cast<uint16_t>(AA_->getModRefInfo(CI, MemoryLocation(SIPointer)));
          uint16_t s1l1 = static_cast<uint16_t>(AA_->getModRefInfo(LI, MemoryLocation(CIPointer)));
	  uint16_t l1s2 = static_cast<uint16_t>(AA_->getModRefInfo(LI, MemoryLocation(SIPointer)));
          uint16_t mustmod = static_cast<uint16_t>(ModRefInfo::MustMod);
	  uint16_t mustref = static_cast<uint16_t>(ModRefInfo::MustRef);
	  uint16_t mod = static_cast<uint16_t>(ModRefInfo::Mod);
	  uint16_t ref = static_cast<uint16_t>(ModRefInfo::Ref);
	  // see the paper for why this combination

	  if(((s1l1 == mustref)&&(l1s2==ref)&&(s1s2==mod))||((s1l1==ref)&&(s1s2 == mustmod)&&(l1s2 == ref))||((s1l1 == mustref)&&(s1s2 == mustmod)&&(l1s2 == mustref))){
            return true;
          }
        }
      }
    }
    // Non-local case.
    // Move the cursor to the end of BB's IDom block.
    
    BasicBlock *BB = SBB;
    DomTreeNode *DTNode = DT_->getNode(BB), *LDTNode = DT_->getNode(LBB);

    // Already tested the local case, so move the cursor to the end of BB's IDom block.
    DTNode = DTNode->getIDom();
    if (DTNode == NULL)
      return false;
    BB = DTNode->getBlock();
    Cursor = BB->end();

    while (DT_->dominates(DTNode, LDTNode)) {
      BasicBlock::iterator E = BB->begin();
      while (Cursor != E)
        if (isa<StoreInst>(--Cursor)){
	    Value *SIPointer = SI->getOperand(1);
            CI = &*Cursor;

	    Value *CIPointer = CI->getOperand(1);

            uint16_t s1s2 = static_cast<uint16_t>(AA_->getModRefInfo(CI, MemoryLocation(SIPointer)));
            uint16_t s1l1 = static_cast<uint16_t>(AA_->getModRefInfo(LI, MemoryLocation(CIPointer)));
            uint16_t l1s2 = static_cast<uint16_t>(AA_->getModRefInfo(LI, MemoryLocation(SIPointer)));

            uint16_t mustmod = static_cast<uint16_t>(ModRefInfo::MustMod);
            uint16_t mustref = static_cast<uint16_t>(ModRefInfo::MustRef);
            uint16_t mod = static_cast<uint16_t>(ModRefInfo::Mod);
            uint16_t ref = static_cast<uint16_t>(ModRefInfo::Ref);
            // see the paper for why this combination

            if(((s1l1 == mustref)&&(l1s2==ref)&&(s1s2==mod))||((s1l1==ref)&&(s1s2 == mustmod)&&(l1s2 == ref))||((s1l1 == mustref)&&(s1s2 == mustmod)&&(l1s2 == mustref))){
                return true;
	    }
        }

      // Move the cursor to the end of BB's IDom block.
      DTNode = DTNode->getIDom();
      if (DTNode == NULL)
        break;
      BB = DTNode->getBlock();
      Cursor = BB->end();
    }
    return false;
}




//===----------------------------------------------------------------------===//
// MemoryIdempotenceAnalysis
//===----------------------------------------------------------------------===//

char MemoryIdempotenceAnalysis::ID = 0;



static RegisterPass<MemoryIdempotenceAnalysis> h1("idempotence-analysis",
        "Idempotence Analysis", true, true);
static void registerMemoryIdempotenceAnalysis(const PassManagerBuilder &,
        legacy::PassManagerBase &PM) {
    PM.add(new MemoryIdempotenceAnalysis());
}
static RegisterStandardPasses RegisterMemoryIdempotenceAnalysis(
        PassManagerBuilder::EP_OptimizerLast, registerMemoryIdempotenceAnalysis);



void MemoryIdempotenceAnalysis::getAnalysisUsage(AnalysisUsage &AU) const{
  AU.addRequired<AAResultsWrapperPass>();
  AU.addRequired<DominatorTreeWrapperPass>();
  AU.addRequired<LoopInfoWrapperPass>();
  AU.setPreservesAll();
}

bool MemoryIdempotenceAnalysis::doInitialization(Module &M) {
  Impl = new MemoryIdempotenceAnalysisImpl(this);
  CutSet_ = &Impl->CutSet_;
  AntidependencePairs_ = &Impl->AntidependencePairs_;
  return false;
}

bool MemoryIdempotenceAnalysis::doFinalization(Module &M) {
  delete Impl;
  return false;
}

void MemoryIdempotenceAnalysis::releaseMemory() {
  Impl->releaseMemory();
}

bool MemoryIdempotenceAnalysis::runOnFunction(Function &F) {
  return Impl->runOnFunction(F);
}


void MemoryIdempotenceAnalysis::print(raw_ostream &OS, const Module *M) const {
  Impl->print(OS, M);
}


