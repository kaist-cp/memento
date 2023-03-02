//===-- PMCPass.cpp - xxx -------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// This file is a modified version of ThreadSanitizer.cpp, a part of a race detector.
//
// The tool is under development, for the details about previous versions see
// http://code.google.com/p/data-race-test
//
// The instrumentation phase is quite simple:
//   - Insert calls to run-time library before every memory access.
//      - Optimizations may apply to avoid instrumenting some of the accesses.
//   - Insert calls at function entry/exit.
// The rest is handled by the run-time library.
//===----------------------------------------------------------------------===//

#include "llvm/ADT/Statistic.h"
#include "llvm/ADT/StringExtras.h"
#include "llvm/ADT/SmallString.h"
#include "llvm/Analysis/ValueTracking.h"
#include "llvm/Analysis/CaptureTracking.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/IntrinsicInst.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/PassManager.h"
#include "llvm/IR/InlineAsm.h"
#include "llvm/Pass.h"
#include "llvm/ProfileData/InstrProf.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Support/AtomicOrdering.h"
#include "llvm/Support/Debug.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Utils/BasicBlockUtils.h"
#include "llvm/Transforms/Utils/EscapeEnumerator.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include <vector>
using namespace llvm;

#define DEBUG_TYPE "PMC"
#include <llvm/IR/DebugLoc.h>

#define ENABLEATOMIC

enum NVMOP {
	NVM_CLWB,
	NVM_FENCE,
	NVM_UNKNOWN
};

typedef enum NVMOP NVMOP;

Value *getPosition( Instruction * I, IRBuilder <> &IRB, bool print = false)
{
	const DebugLoc & debug_location = I->getDebugLoc ();
	std::string position_string;
	{
		llvm::raw_string_ostream position_stream (position_string);
		debug_location . print (position_stream);
	}

	if (print) {
		errs() << position_string << "\n";
	}

	return IRB.CreateGlobalStringPtr (position_string);
}

STATISTIC(NumInstrumentedReads, "Number of instrumented reads");
STATISTIC(NumInstrumentedWrites, "Number of instrumented writes");
STATISTIC(NumOmittedReadsBeforeWrite,
          "Number of reads ignored due to following writes");
STATISTIC(NumAccessesWithBadSize, "Number of accesses with bad size");
// STATISTIC(NumInstrumentedVtableWrites, "Number of vtable ptr writes");
// STATISTIC(NumInstrumentedVtableReads, "Number of vtable ptr reads");
STATISTIC(NumOmittedReadsFromConstantGlobals,
          "Number of reads from constant globals");
STATISTIC(NumOmittedReadsFromVtable, "Number of vtable reads");
STATISTIC(NumOmittedNonCaptured, "Number of accesses ignored due to capturing");

// static const char *const kCDSModuleCtorName = "cds.module_ctor";
// static const char *const kCDSInitName = "cds_init";

Type * OrdTy;
Type * IntPtrTy;
Type * Int8PtrTy;
Type * Int16PtrTy;
Type * Int32PtrTy;
Type * Int64PtrTy;

Type * VoidTy;

static const size_t kNumberOfAccessSizes = 4;
#ifdef ENABLEATOMIC
int getAtomicOrderIndex(AtomicOrdering order) {
	switch (order) {
		case AtomicOrdering::Monotonic: 
			return (int)AtomicOrderingCABI::relaxed;
		//  case AtomicOrdering::Consume:         // not specified yet
		//    return AtomicOrderingCABI::consume;
		case AtomicOrdering::Acquire: 
			return (int)AtomicOrderingCABI::acquire;
		case AtomicOrdering::Release: 
			return (int)AtomicOrderingCABI::release;
		case AtomicOrdering::AcquireRelease: 
			return (int)AtomicOrderingCABI::acq_rel;
		case AtomicOrdering::SequentiallyConsistent: 
			return (int)AtomicOrderingCABI::seq_cst;
		default:
			// unordered or Not Atomic
			return -1;
	}
}

AtomicOrderingCABI indexToAtomicOrder(int index) {
	switch (index) {
		case 0:
			return AtomicOrderingCABI::relaxed;
		case 1:
			return AtomicOrderingCABI::consume;
		case 2:
			return AtomicOrderingCABI::acquire;
		case 3:
			return AtomicOrderingCABI::release;
		case 4:
			return AtomicOrderingCABI::acq_rel;
		case 5:
			return AtomicOrderingCABI::seq_cst;
		default:
			errs() << "Bad Atomic index\n";
			return AtomicOrderingCABI::seq_cst;
	}
}

/* According to atomic_base.h: __cmpexch_failure_order */
int AtomicCasFailureOrderIndex(int index) {
	AtomicOrderingCABI succ_order = indexToAtomicOrder(index);
	AtomicOrderingCABI fail_order;
	if (succ_order == AtomicOrderingCABI::acq_rel)
		fail_order = AtomicOrderingCABI::acquire;
	else if (succ_order == AtomicOrderingCABI::release) 
		fail_order = AtomicOrderingCABI::relaxed;
	else
		fail_order = succ_order;

	return (int) fail_order;
}
#endif
/* The original function checkSanitizerInterfaceFunction was defined
 * in llvm/Transforms/Utils/ModuleUtils.h
 */
static Function * checkPMCPassInterfaceFunction(Value *FuncOrBitcast) {
	if (isa<Function>(FuncOrBitcast))
		return cast<Function>(FuncOrBitcast);
	FuncOrBitcast->print(errs());
	errs() << '\n';
	std::string Err;
	raw_string_ostream Stream(Err);
	Stream << "PMCPass interface function redefined: " << *FuncOrBitcast;
	report_fatal_error(Err);
} 

namespace {

	struct PMCPass : public FunctionPass {
		PMCPass() : FunctionPass(ID) {}
		StringRef getPassName() const override;
		bool runOnFunction(Function &F) override;
		bool doInitialization(Module &M) override;
		static char ID;

	private:
		void instrumentFenceOp(Instruction *I, const DataLayout &DL);
		bool instrumentCacheOp(Instruction *I, const DataLayout &DL);
		void initializeCallbacks(Module &M);
		bool instrumentLoadOrStore(Instruction *I, const DataLayout &DL);
		bool instrumentMemIntrinsic(Instruction *I);
		NVMOP whichNVMoperation(Instruction *I);
		Function * whichNVMFunction(Instruction *I);
#ifdef ENABLEATOMIC
		bool instrumentVolatile(Instruction *I, const DataLayout &DL);
		bool isAtomicCall(Instruction *I);
		bool instrumentAtomic(Instruction *I, const DataLayout &DL);
		bool instrumentAtomicCall(CallInst *CI, const DataLayout &DL);
		bool shouldInstrumentBeforeAtomics(Instruction *I);
#endif
		void chooseInstructionsToInstrument(SmallVectorImpl<Instruction *> &Local,
											SmallVectorImpl<Instruction *> &All,
											const DataLayout &DL);
		bool addrPointsToConstantData(Value *Addr);
		int getMemoryAccessFuncIndex(Value *Addr, const DataLayout &DL);

		//Function * PMCFuncEntry;
		//Function * PMCFuncExit;

		Function * PMCLoad[kNumberOfAccessSizes];
		Function * PMCStore[kNumberOfAccessSizes];
		Function * PMCVolatileLoad[kNumberOfAccessSizes];
		Function * PMCVolatileStore[kNumberOfAccessSizes];
#ifdef ENABLEATOMIC
		Function * PMCAtomicInit[kNumberOfAccessSizes];
		Function * PMCAtomicLoad[kNumberOfAccessSizes];
		Function * PMCAtomicStore[kNumberOfAccessSizes];
		Function * PMCAtomicRMW[AtomicRMWInst::LAST_BINOP + 1][kNumberOfAccessSizes];
		Function * PMCAtomicCAS_V1[kNumberOfAccessSizes];
		Function * PMCAtomicCAS_V2[kNumberOfAccessSizes];
		Function * PMCAtomicThreadFence;
#endif
		Function * MemmoveFn, * MemcpyFn, * MemsetFn;
		Function *CLFlushFn, *CLFlushOptFn, *CLWBFn;
		Function *SFenceFn, *MFenceFn, *LFenceFn;
#ifdef ENABLEATOMIC
		std::vector<StringRef> AtomicFuncNames;
#endif
		std::vector<StringRef> PartialAtomicFuncNames;
		std::vector<StringRef> CacheOperationsNames;
		std::vector<StringRef> FenceOperationsNames;
	};

}

StringRef PMCPass::getPassName() const {
	return "PMCPass";
}

Function * PMCPass::whichNVMFunction(Instruction *I){
	if(CallInst* callInst = dyn_cast<CallInst>(I)) {
		if(callInst->isInlineAsm()){
			InlineAsm *asmInline = dyn_cast<InlineAsm>(callInst->getCalledOperand());
			StringRef asmStr = asmInline->getAsmString();
			if (asmStr.contains("clflushopt")) {
				return CLFlushOptFn;
			} else if (asmStr.contains("clflush")) {
				return CLFlushFn;
			} else if (asmStr.contains("xsaveopt") ){
				return CLWBFn;
			} else if (asmStr.contains("mfence")) {
				return MFenceFn;
			} else if (asmStr.contains("sfence")) {
				return SFenceFn;
			} else if (asmStr.contains("lfence")) {
				return LFenceFn;
			}

		}
	}
	return NULL;
}


NVMOP PMCPass::whichNVMoperation(Instruction *I){
	if(CallInst* callInst = dyn_cast<CallInst>(I)) {
		if(callInst->isInlineAsm()){
			InlineAsm *asmInline = dyn_cast<InlineAsm>(callInst->getCalledOperand());
			StringRef asmStr = asmInline->getAsmString();
			for( StringRef op : FenceOperationsNames){
				if(asmStr.contains(op))
					return NVM_FENCE;
			}
			for( StringRef op : CacheOperationsNames){
				if(asmStr.contains(op))
					return NVM_CLWB;
			}
		}
	}
	return NVM_UNKNOWN;
}

void PMCPass::initializeCallbacks(Module &M) {
	LLVMContext &Ctx = M.getContext();
	AttributeList Attr;
	Attr = Attr.addAttribute(Ctx, AttributeList::FunctionIndex,
			Attribute::NoUnwind);
#ifdef ENABLEATOMIC
	Type * Int1Ty = Type::getInt1Ty(Ctx);
#endif
	Type * Int32Ty = Type::getInt32Ty(Ctx);
	OrdTy = Type::getInt32Ty(Ctx);

	Int8PtrTy  = Type::getInt8PtrTy(Ctx);
	Int16PtrTy = Type::getInt16PtrTy(Ctx);
	Int32PtrTy = Type::getInt32PtrTy(Ctx);
	Int64PtrTy = Type::getInt64PtrTy(Ctx);

	VoidTy = Type::getVoidTy(Ctx);
	// Get the function to call from our untime library.
	for (unsigned i = 0; i < kNumberOfAccessSizes; i++) {
		const unsigned ByteSize = 1U << i;
		const unsigned BitSize = ByteSize * 8;

		std::string ByteSizeStr = utostr(ByteSize);
		std::string BitSizeStr = utostr(BitSize);

		Type *Ty = Type::getIntNTy(Ctx, BitSize);
		Type *PtrTy = Ty->getPointerTo();

		SmallString<32> LoadName("pmc_load" + BitSizeStr);
		SmallString<32> StoreName("pmc_store" + BitSizeStr);
		
#ifdef ENABLEATOMIC
		SmallString<32> VolatileLoadName("pmc_volatile_load" + BitSizeStr);
		SmallString<32> VolatileStoreName("pmc_volatile_store" + BitSizeStr);
		SmallString<32> AtomicInitName("pmc_atomic_init" + BitSizeStr);
		SmallString<32> AtomicLoadName("pmc_atomic_load" + BitSizeStr);
		SmallString<32> AtomicStoreName("pmc_atomic_store" + BitSizeStr);
#endif
		PMCLoad[i]  = checkPMCPassInterfaceFunction(
							M.getOrInsertFunction(LoadName, Attr, Ty, PtrTy, Int8PtrTy).getCallee());
		PMCStore[i] = checkPMCPassInterfaceFunction(
							M.getOrInsertFunction(StoreName, Attr, VoidTy, PtrTy, Ty, Int8PtrTy).getCallee());
		
#ifdef ENABLEATOMIC		
		PMCVolatileLoad[i]  = checkPMCPassInterfaceFunction(
								M.getOrInsertFunction(VolatileLoadName,
								Attr, Ty, PtrTy, Int8PtrTy).getCallee());
		PMCVolatileStore[i] = checkPMCPassInterfaceFunction(
								M.getOrInsertFunction(VolatileStoreName, 
								Attr, VoidTy, PtrTy, Ty, Int8PtrTy).getCallee());
		
		PMCAtomicInit[i] = checkPMCPassInterfaceFunction(
							M.getOrInsertFunction(AtomicInitName, 
							Attr, VoidTy, PtrTy, Ty, Int8PtrTy).getCallee());
		PMCAtomicLoad[i]  = checkPMCPassInterfaceFunction(
								M.getOrInsertFunction(AtomicLoadName, 
								Attr, Ty, PtrTy, OrdTy, Int8PtrTy).getCallee());
		PMCAtomicStore[i] = checkPMCPassInterfaceFunction(
								M.getOrInsertFunction(AtomicStoreName, 
								Attr, VoidTy, PtrTy, Ty, OrdTy, Int8PtrTy).getCallee());

		for (int op = AtomicRMWInst::FIRST_BINOP; 
			op <= AtomicRMWInst::LAST_BINOP; ++op) {
			PMCAtomicRMW[op][i] = nullptr;
			std::string NamePart;

			if (op == AtomicRMWInst::Xchg)
				NamePart = "_exchange";
			else if (op == AtomicRMWInst::Add) 
				NamePart = "_fetch_add";
			else if (op == AtomicRMWInst::Sub)
				NamePart = "_fetch_sub";
			else if (op == AtomicRMWInst::And)
				NamePart = "_fetch_and";
			else if (op == AtomicRMWInst::Or)
				NamePart = "_fetch_or";
			else if (op == AtomicRMWInst::Xor)
				NamePart = "_fetch_xor";
			else
				continue;

			SmallString<32> AtomicRMWName("pmc_atomic" + NamePart + BitSizeStr);
			PMCAtomicRMW[op][i] = checkPMCPassInterfaceFunction(
									M.getOrInsertFunction(AtomicRMWName, 
									Attr, Ty, PtrTy, Ty, OrdTy, Int8PtrTy).getCallee());
		}

		// only supportes strong version
		SmallString<32> AtomicCASName_V1("pmc_atomic_compare_exchange" + BitSizeStr + "_v1");
		SmallString<32> AtomicCASName_V2("pmc_atomic_compare_exchange" + BitSizeStr + "_v2");
		PMCAtomicCAS_V1[i] = checkPMCPassInterfaceFunction(
								M.getOrInsertFunction(AtomicCASName_V1, 
								Attr, Ty, PtrTy, Ty, Ty, OrdTy, OrdTy, Int8PtrTy).getCallee());
		PMCAtomicCAS_V2[i] = checkPMCPassInterfaceFunction(
								M.getOrInsertFunction(AtomicCASName_V2, 
								Attr, Int1Ty, PtrTy, PtrTy, Ty, OrdTy, OrdTy, Int8PtrTy).getCallee());
#endif
	}
#ifdef ENABLEATOMIC
	PMCAtomicThreadFence = checkPMCPassInterfaceFunction(
			M.getOrInsertFunction("pmc_atomic_thread_fence", Attr, VoidTy, OrdTy, Int8PtrTy).getCallee());
#endif
	
	MemmoveFn = checkPMCPassInterfaceFunction(
					M.getOrInsertFunction("memmove", Attr, Int8PtrTy, Int8PtrTy,
					Int8PtrTy, IntPtrTy).getCallee());
	MemcpyFn = checkPMCPassInterfaceFunction(
					M.getOrInsertFunction("memcpy", Attr, Int8PtrTy, Int8PtrTy,
					Int8PtrTy, IntPtrTy).getCallee());
	MemsetFn = checkPMCPassInterfaceFunction(
					M.getOrInsertFunction("memset", Attr, Int8PtrTy, Int8PtrTy,
					Int32Ty, IntPtrTy).getCallee());
	CLWBFn  = checkPMCPassInterfaceFunction(M.getOrInsertFunction("pmc_clwb", Attr, VoidTy, Int8PtrTy).getCallee());
	CLFlushFn  = checkPMCPassInterfaceFunction(M.getOrInsertFunction("pmc_clflush", Attr, VoidTy, Int8PtrTy).getCallee());
	CLFlushOptFn  = checkPMCPassInterfaceFunction(M.getOrInsertFunction("pmc_clflushopt", Attr, VoidTy, Int8PtrTy).getCallee());
	MFenceFn  = checkPMCPassInterfaceFunction(M.getOrInsertFunction("pmc_mfence", Attr, VoidTy).getCallee());
	SFenceFn  = checkPMCPassInterfaceFunction(M.getOrInsertFunction("pmc_sfence", Attr, VoidTy).getCallee());
	LFenceFn  = checkPMCPassInterfaceFunction(M.getOrInsertFunction("pmc_lfence", Attr, VoidTy).getCallee());
}

bool PMCPass::doInitialization(Module &M) {
	const DataLayout &DL = M.getDataLayout();
	IntPtrTy = DL.getIntPtrType(M.getContext());
	
	// createSanitizerCtorAndInitFunctions is defined in "llvm/Transforms/Utils/ModuleUtils.h"
	// We do not support it yet
	/*
	std::tie(CDSCtorFunction, std::ignore) = createSanitizerCtorAndInitFunctions(
			M, kCDSModuleCtorName, kCDSInitName, {}, {});

	appendToGlobalCtors(M, CDSCtorFunction, 0);
	*/
#ifdef ENABLEATOMIC
	AtomicFuncNames = 
	{
		"atomic_init", "atomic_load", "atomic_store", 
		"atomic_fetch_", "atomic_exchange", "atomic_compare_exchange_"
	};
#endif
	PartialAtomicFuncNames = 
	{ 
		"load", "store"
#ifdef ENABLEATOMIC
		, "fetch", "exchange", "compare_exchange_"
#endif
	};
	
	CacheOperationsNames =
	{
		"clflush", "xsaveopt", "clflushopt"
	};

	FenceOperationsNames = 
	{
		"mfence", "sfence", "lfence"
	};
	return true;
}

static bool isVtableAccess(Instruction *I) {
	if (MDNode *Tag = I->getMetadata(LLVMContext::MD_tbaa))
		return Tag->isTBAAVtableAccess();
	return false;
}

// Do not instrument known races/"benign races" that come from compiler
// instrumentatin. The user has no way of suppressing them.
static bool shouldInstrumentReadWriteFromAddress(const Module *M, Value *Addr) {
	// Peel off GEPs and BitCasts.
	Addr = Addr->stripInBoundsOffsets();

	if (GlobalVariable *GV = dyn_cast<GlobalVariable>(Addr)) {
		if (GV->hasSection()) {
			StringRef SectionName = GV->getSection();
			// Check if the global is in the PGO counters section.
			auto OF = Triple(M->getTargetTriple()).getObjectFormat();
			if (SectionName.endswith(
			      getInstrProfSectionName(IPSK_cnts, OF, /*AddSegmentInfo=*/false)))
				return false;
		}

		// Check if the global is private gcov data.
		if (GV->getName().startswith("__llvm_gcov") ||
		GV->getName().startswith("__llvm_gcda"))
		return false;
	}

	// Do not instrument acesses from different address spaces; we cannot deal
	// with them.
	if (Addr) {
		Type *PtrTy = cast<PointerType>(Addr->getType()->getScalarType());
		if (PtrTy->getPointerAddressSpace() != 0)
			return false;
	}

	return true;
}

bool PMCPass::addrPointsToConstantData(Value *Addr) {
	// If this is a GEP, just analyze its pointer operand.
	if (GetElementPtrInst *GEP = dyn_cast<GetElementPtrInst>(Addr))
		Addr = GEP->getPointerOperand();

	if (GlobalVariable *GV = dyn_cast<GlobalVariable>(Addr)) {
		if (GV->isConstant()) {
			// Reads from constant globals can not race with any writes.
			NumOmittedReadsFromConstantGlobals++;
			return true;
		}
	} else if (LoadInst *L = dyn_cast<LoadInst>(Addr)) {
		if (isVtableAccess(L)) {
			// Reads from a vtable pointer can not race with any writes.
			NumOmittedReadsFromVtable++;
			return true;
		}
	}
	return false;
}
#ifdef ENABLEATOMIC
bool PMCPass::shouldInstrumentBeforeAtomics(Instruction * Inst) {
	if (LoadInst *LI = dyn_cast<LoadInst>(Inst)) {
		AtomicOrdering ordering = LI->getOrdering();
		if ( isAtLeastOrStrongerThan(ordering, AtomicOrdering::Acquire) )
			return true;
	} else if (StoreInst *SI = dyn_cast<StoreInst>(Inst)) {
		AtomicOrdering ordering = SI->getOrdering();
		if ( isAtLeastOrStrongerThan(ordering, AtomicOrdering::Acquire) )
			return true;
	} else if (AtomicRMWInst *RMWI = dyn_cast<AtomicRMWInst>(Inst)) {
		AtomicOrdering ordering = RMWI->getOrdering();
		if ( isAtLeastOrStrongerThan(ordering, AtomicOrdering::Acquire) )
			return true;
	} else if (AtomicCmpXchgInst *CASI = dyn_cast<AtomicCmpXchgInst>(Inst)) {
		AtomicOrdering ordering = CASI->getSuccessOrdering();
		if ( isAtLeastOrStrongerThan(ordering, AtomicOrdering::Acquire) )
			return true;
	} else if (FenceInst *FI = dyn_cast<FenceInst>(Inst)) {
		AtomicOrdering ordering = FI->getOrdering();
		if ( isAtLeastOrStrongerThan(ordering, AtomicOrdering::Acquire) )
			return true;
	}

	return false;
}
#endif
void PMCPass::chooseInstructionsToInstrument(
	SmallVectorImpl<Instruction *> &Local, SmallVectorImpl<Instruction *> &All,
	const DataLayout &DL) {
	SmallPtrSet<Value*, 8> WriteTargets;
	SmallVector<Instruction*, 8> results;
	// Iterate from the end.
	for (Instruction *I : reverse(Local)) {
		if (StoreInst *Store = dyn_cast<StoreInst>(I)) {
			Value *Addr = Store->getPointerOperand();
			if (!shouldInstrumentReadWriteFromAddress(I->getModule(), Addr)){
				errs() << "Store: Should...." << '\n';
				continue;
			}
			WriteTargets.insert(Addr);
		} else {
			LoadInst *Load = cast<LoadInst>(I);
			Value *Addr = Load->getPointerOperand();
			if (!shouldInstrumentReadWriteFromAddress(I->getModule(), Addr)){
				errs() << "Load: Should...." << '\n';
				continue;
			}
			//if (WriteTargets.count(Addr)) {
				// We will write to this temp, so no reason to analyze the read.
			//	NumOmittedReadsBeforeWrite++;
			//	continue;
			//}
			if (addrPointsToConstantData(Addr)) {
				errs() << "Load: Constant ..." << '\n';
				// Addr points to some constant data -- it can not race with any writes.
				continue;
			}
		}
		Value *Addr = isa<StoreInst>(*I)
			? cast<StoreInst>(I)->getPointerOperand()
			: cast<LoadInst>(I)->getPointerOperand();
		if (isa<AllocaInst>(GetUnderlyingObject(Addr, DL)) &&
				!PointerMayBeCaptured(Addr, true, true)) {
			// The variable is addressable but not captured, so it cannot be
			// referenced from a different thread and participate in a data race
			// (see llvm/Analysis/CaptureTracking.h for details).
			
			//errs() << "Captured ..." << *Addr << '\n';
			NumOmittedNonCaptured++;
			continue;
		}
		results.push_back(I);
	}
	for( Instruction *I: reverse(results) ){
		All.push_back(I);
	}
	Local.clear();
}

/* Not implemented
void CDSPass::InsertRuntimeIgnores(Function &F) {
	IRBuilder<> IRB(F.getEntryBlock().getFirstNonPHI());
	IRB.CreateCall(CDSIgnoreBegin);
	EscapeEnumerator EE(F, "cds_ignore_cleanup", ClHandleCxxExceptions);
	while (IRBuilder<> *AtExit = EE.Next()) {
		AtExit->CreateCall(CDSIgnoreEnd);
	}
}*/

bool PMCPass::runOnFunction(Function &F) {
	initializeCallbacks( *F.getParent() );
	SmallVector<Instruction*, 8> AllLoadsAndStores;
	SmallVector<Instruction*, 8> FenceOperations;
	SmallVector<Instruction*, 8> CacheOperations;
	SmallVector<Instruction*, 8> LocalLoadsAndStores;
#ifdef ENABLEATOMIC
	SmallVector<Instruction*, 8> VolatileLoadsAndStores;
	SmallVector<Instruction*, 8> AtomicAccesses;
#endif
	SmallVector<Instruction*, 8> MemIntrinCalls;

	const DataLayout &DL = F.getParent()->getDataLayout();

	for (auto &BB : F) {
		for (auto &Inst : BB) {
#ifdef ENABLEATOMIC
			if ( (&Inst)->isAtomic() ) {
				AtomicAccesses.push_back(&Inst);

				if (shouldInstrumentBeforeAtomics(&Inst)) {
					chooseInstructionsToInstrument(LocalLoadsAndStores, AllLoadsAndStores,
						DL);
				}
			} else if (isAtomicCall(&Inst) ) {
				AtomicAccesses.push_back(&Inst);
				chooseInstructionsToInstrument(LocalLoadsAndStores, AllLoadsAndStores,
					DL);
			} else 
#endif
			if (isa<LoadInst>(Inst) || isa<StoreInst>(Inst)) {
				LoadInst *LI = dyn_cast<LoadInst>(&Inst);
				StoreInst *SI = dyn_cast<StoreInst>(&Inst);
				bool isVolatile = ( LI ? LI->isVolatile() : SI->isVolatile() );

				if (isVolatile) {
#ifdef ENABLEATOMIC
					VolatileLoadsAndStores.push_back(&Inst);
#endif
				} else
					LocalLoadsAndStores.push_back(&Inst);
			} else if (isa<CallInst>(Inst) || isa<InvokeInst>(Inst)) {
				if (isa<MemIntrinsic>(Inst))
					MemIntrinCalls.push_back(&Inst);
				else{
					NVMOP op = whichNVMoperation(&Inst);
					if(op == NVM_FENCE){
						FenceOperations.push_back(&Inst);
					} else if (op == NVM_CLWB) {
						CacheOperations.push_back(&Inst);
					}
				}
				/*if (CallInst *CI = dyn_cast<CallInst>(&Inst))
					maybeMarkSanitizerLibraryCallNoBuiltin(CI, TLI);
				*/

				chooseInstructionsToInstrument(LocalLoadsAndStores, AllLoadsAndStores,
					DL);
			}
		}

		chooseInstructionsToInstrument(LocalLoadsAndStores, AllLoadsAndStores, DL);
	}

	for (auto Inst : AllLoadsAndStores) {
		instrumentLoadOrStore(Inst, DL);
	}

#ifdef ENABLEATOMIC 	
	for (auto Inst : VolatileLoadsAndStores) {
		instrumentVolatile(Inst, DL);
	}
	for (auto Inst : AtomicAccesses) {
		instrumentAtomic(Inst, DL);
	}
#endif
	
	for (auto Inst : MemIntrinCalls) {
		instrumentMemIntrinsic(Inst);
	}

	for (auto Inst : CacheOperations) {
		instrumentCacheOp(Inst, DL);
	}

	for (auto Inst : FenceOperations) {
		instrumentFenceOp(Inst, DL);
	}
	
	return false;
}

bool PMCPass::instrumentCacheOp( Instruction *I, const DataLayout &DL){
	errs() << "Instrumenting CacheOp: " << *I << "\n";
	IRBuilder<> IRB(I);
	assert(isa<CallInst>(I));
	CallInst * CI = dyn_cast<CallInst>(I);
        std::vector<Value *> parameters;
        User::op_iterator begin = CI->arg_begin();
	User::op_iterator end = CI->arg_end();
	Function *CacheFn = whichNVMFunction(I);
        for (User::op_iterator it = begin; it != end; ++it) {
                Value *param = *it;
                parameters.push_back(param);
        }

	if(parameters.size() != 2){
		return false;
	}
	Value *Addr = parameters[0];
	if(Addr->isSwiftError()){
		return false;
	}
	int Idx = getMemoryAccessFuncIndex(Addr, DL);
	if (Idx < 0)
		return false;

	Type *ArgType = IRB.CreatePointerCast(Addr, Addr->getType())->getType();
	if ( ArgType != Int8PtrTy ) {
                return false;
        }
	//IRB.CreateCall(CacheFn, IRB.CreatePointerCast(Addr, Addr->getType()));
	Value *args[] = {Addr};
	Instruction* funcInst = CallInst::Create(CacheFn, args);
	ReplaceInstWithInst(I, funcInst);
	return true;
}

void PMCPass::instrumentFenceOp(Instruction *I, const DataLayout &DL){
	errs() << "Intrumenting Cache Fence: " << *I << "\n";
	IRBuilder<> IRB(I);
	Function *FenceFn = whichNVMFunction(I);
	IRB.CreateCall(FenceFn);
}


bool PMCPass::instrumentLoadOrStore(Instruction *I, const DataLayout &DL) {
	IRBuilder<> IRB(I);
	Value *position = getPosition(I, IRB, true);
	bool IsWrite = isa<StoreInst>(*I);
	Value *addr = IsWrite
		? cast<StoreInst>(I)->getPointerOperand()
		: cast<LoadInst>(I)->getPointerOperand();

	// swifterror memory addresses are mem2reg promoted by instruction selection.
	// As such they cannot have regular uses like an instrumentation function and
	// it makes no sense to track them as memory.
	if (addr->isSwiftError())
		return false;

	int idx = getMemoryAccessFuncIndex(addr, DL);
	if (idx < 0)
		return false;

	if (IsWrite && isVtableAccess(I)) {
		/* TODO
		LLVM_DEBUG(dbgs() << "	VPTR : " << *I << "\n");
		Value *StoredValue = cast<StoreInst>(I)->getValueOperand();
		// StoredValue may be a vector type if we are storing several vptrs at once.
		// In this case, just take the first element of the vector since this is
		// enough to find vptr races.
		if (isa<VectorType>(StoredValue->getType()))
			StoredValue = IRB.CreateExtractElement(
					StoredValue, ConstantInt::get(IRB.getInt32Ty(), 0));
		if (StoredValue->getType()->isIntegerTy())
			StoredValue = IRB.CreateIntToPtr(StoredValue, IRB.getInt8PtrTy());
		// Call TsanVptrUpdate.
		IRB.CreateCall(TsanVptrUpdate,
						{IRB.CreatePointerCast(Addr, IRB.getInt8PtrTy()),
							IRB.CreatePointerCast(StoredValue, IRB.getInt8PtrTy())});
		NumInstrumentedVtableWrites++;
		*/
		return true;
	}

	if (!IsWrite && isVtableAccess(I)) {
		/* TODO
		IRB.CreateCall(TsanVptrLoad,
						 IRB.CreatePointerCast(Addr, IRB.getInt8PtrTy()));
		NumInstrumentedVtableReads++;
		*/
		return true;
	}

	// TODO: unaligned reads and writes
	
	Value *OnAccessFunc = IsWrite ? PMCStore[idx] : PMCLoad[idx];
	const unsigned byteSize = 1U << idx;
        const unsigned bitSize = byteSize * 8;
        Type *Ty = Type::getIntNTy(IRB.getContext(), bitSize);
        Type *ptrTy = Ty->getPointerTo();

	if (StoreInst * SI = dyn_cast<StoreInst>(I)) {
		errs() << "Instrumenting non-atomic store: " << *I << "\n";
		Value *val = SI->getValueOperand();
		Value *args[] = {IRB.CreatePointerCast(addr, ptrTy),
                                IRB.CreateBitOrPointerCast(val, Ty), position};
                CallInst *C = CallInst::Create(OnAccessFunc, args);
                ReplaceInstWithInst(I, C);

		NumInstrumentedWrites++;
	} else if (!IsWrite){
		errs() << "Instrumenting non-atomic load: " << *I << "\n";
		Value *args[] = {IRB.CreatePointerCast(addr, ptrTy), position};
                Type *orgTy = cast<PointerType>(addr->getType())->getElementType();
                Value *funcInst = IRB.CreateCall(OnAccessFunc, args);
                Value *cast = IRB.CreateBitOrPointerCast(funcInst, orgTy);
                I->replaceAllUsesWith(cast);
		
		NumInstrumentedReads++;
	} else {
		return false;
	}

	return true;
}

#ifdef ENABLEATOMIC
bool PMCPass::instrumentVolatile(Instruction * I, const DataLayout &DL) {
	errs() << "Intrumenting Volatile: " << *I << "\tNumber of operands: "<< I->getNumOperands() <<"\n";
	IRBuilder<> IRB(I);
	Value *position = getPosition(I, IRB, true);
	bool IsWrite = isa<StoreInst>(*I);
        Value *addr = IsWrite
                ? cast<StoreInst>(I)->getPointerOperand()
                : cast<LoadInst>(I)->getPointerOperand();
	int idx=getMemoryAccessFuncIndex(addr, DL);
	if (idx < 0)
		return false;
	const unsigned byteSize = 1U << idx;
	const unsigned bitSize = byteSize * 8;
        Type *Ty = Type::getIntNTy(IRB.getContext(), bitSize);
	Type *ptrTy = Ty->getPointerTo();
	if (LoadInst *LI = dyn_cast<LoadInst>(I)) {
		assert( LI->isVolatile() );
		Value *args[] = {IRB.CreatePointerCast(addr, ptrTy), position};
		Type *orgTy = cast<PointerType>(addr->getType())->getElementType();
		Value *funcInst = IRB.CreateCall(PMCVolatileLoad[idx], args);
		Value *cast = IRB.CreateBitOrPointerCast(funcInst, orgTy);
		I->replaceAllUsesWith(cast);
	} else if (StoreInst *SI = dyn_cast<StoreInst>(I)) {
		assert( SI->isVolatile() );
		Value *val = SI->getValueOperand();
		//assert(PMCVolatileStore[idx]->arg_size()> 2);
		//Type *valType = PMCVolatileStore[idx]->getArg(1)->getType();
		//if( val->getType() != valType) {
                //        errs() << "PMC Warning: Value Type= " << *val->getType() << "\t casting to ==>" << *valType << '\n';
		//	val = CastInst::CreatePointerCast(val, valType);
		//}
		Value *args[] = {IRB.CreatePointerCast(addr, ptrTy),
				IRB.CreateBitOrPointerCast(val, Ty), position};
		CallInst *C = CallInst::Create(PMCVolatileStore[idx], args);
		ReplaceInstWithInst(I, C);
	} else {
		return false;
	}
	return true;
}
#endif

bool PMCPass::instrumentMemIntrinsic(Instruction *I) {
	errs() << "Intrumenting Memory: " << *I << "\n";
	IRBuilder<> IRB(I);
	if (MemSetInst *M = dyn_cast<MemSetInst>(I)) {
		IRB.CreateCall(
			MemsetFn,
			{IRB.CreatePointerCast(M->getArgOperand(0), IRB.getInt8PtrTy()),
			 IRB.CreateIntCast(M->getArgOperand(1), IRB.getInt32Ty(), false),
			 IRB.CreateIntCast(M->getArgOperand(2), IntPtrTy, false)});
		I->eraseFromParent();
	} else if (MemTransferInst *M = dyn_cast<MemTransferInst>(I)) {
		IRB.CreateCall(
			isa<MemCpyInst>(M) ? MemcpyFn : MemmoveFn,
			{IRB.CreatePointerCast(M->getArgOperand(0), IRB.getInt8PtrTy()),
			 IRB.CreatePointerCast(M->getArgOperand(1), IRB.getInt8PtrTy()),
			 IRB.CreateIntCast(M->getArgOperand(2), IntPtrTy, false)});
		I->eraseFromParent();
	}
	return true;
}


#ifdef ENABLEATOMIC
bool PMCPass::instrumentAtomic(Instruction * I, const DataLayout &DL) {
	errs() << "Intrumenting Atomic: " << *I << "\n";
	IRBuilder<> IRB(I);

	if (auto *CI = dyn_cast<CallInst>(I)) {
		return instrumentAtomicCall(CI, DL);
	}

	Value *position = getPosition(I, IRB, true);

	if (LoadInst *LI = dyn_cast<LoadInst>(I)) {
		Value *Addr = LI->getPointerOperand();
		int Idx=getMemoryAccessFuncIndex(Addr, DL);
		if (Idx < 0)
			return false;

		int atomic_order_index = getAtomicOrderIndex(LI->getOrdering());
		Value *order = ConstantInt::get(OrdTy, atomic_order_index);
		Value *args[] = {Addr, order, position};
		Instruction* funcInst = CallInst::Create(PMCAtomicLoad[Idx], args);
		ReplaceInstWithInst(LI, funcInst);
	} else if (StoreInst *SI = dyn_cast<StoreInst>(I)) {
		Value *Addr = SI->getPointerOperand();
		int Idx=getMemoryAccessFuncIndex(Addr, DL);
		if (Idx < 0)
			return false;

		int atomic_order_index = getAtomicOrderIndex(SI->getOrdering());
		Value *val = SI->getValueOperand();
		Value *order = ConstantInt::get(OrdTy, atomic_order_index);
		Value *args[] = {Addr, val, order, position};
		Instruction* funcInst = CallInst::Create(PMCAtomicStore[Idx], args);
		ReplaceInstWithInst(SI, funcInst);
	} else if (AtomicRMWInst *RMWI = dyn_cast<AtomicRMWInst>(I)) {
		Value *Addr = RMWI->getPointerOperand();
		int Idx=getMemoryAccessFuncIndex(Addr, DL);
		if (Idx < 0)
			return false;

		int atomic_order_index = getAtomicOrderIndex(RMWI->getOrdering());
		Value *val = RMWI->getValOperand();
		Value *order = ConstantInt::get(OrdTy, atomic_order_index);
		Value *args[] = {Addr, val, order, position};
		Instruction* funcInst = CallInst::Create(PMCAtomicRMW[RMWI->getOperation()][Idx], args);
		ReplaceInstWithInst(RMWI, funcInst);
	} else if (AtomicCmpXchgInst *CASI = dyn_cast<AtomicCmpXchgInst>(I)) {
		IRBuilder<> IRB(CASI);

		Value *Addr = CASI->getPointerOperand();
		int Idx=getMemoryAccessFuncIndex(Addr, DL);
		if (Idx < 0)
			return false;

		const unsigned ByteSize = 1U << Idx;
		const unsigned BitSize = ByteSize * 8;
		Type *Ty = Type::getIntNTy(IRB.getContext(), BitSize);
		Type *PtrTy = Ty->getPointerTo();

		Value *CmpOperand = IRB.CreateBitOrPointerCast(CASI->getCompareOperand(), Ty);
		Value *NewOperand = IRB.CreateBitOrPointerCast(CASI->getNewValOperand(), Ty);

		int atomic_order_index_succ = getAtomicOrderIndex(CASI->getSuccessOrdering());
		int atomic_order_index_fail = getAtomicOrderIndex(CASI->getFailureOrdering());
		Value *order_succ = ConstantInt::get(OrdTy, atomic_order_index_succ);
		Value *order_fail = ConstantInt::get(OrdTy, atomic_order_index_fail);

		Value *Args[] = {IRB.CreatePointerCast(Addr, PtrTy),
						 CmpOperand, NewOperand,
						 order_succ, order_fail, position};

		CallInst *funcInst = IRB.CreateCall(PMCAtomicCAS_V1[Idx], Args);
		Value *Success = IRB.CreateICmpEQ(funcInst, CmpOperand);

		Value *OldVal = funcInst;
		Type *OrigOldValTy = CASI->getNewValOperand()->getType();
		if (Ty != OrigOldValTy) {
			// The value is a pointer, so we need to cast the return value.
			OldVal = IRB.CreateIntToPtr(funcInst, OrigOldValTy);
		}

		Value *Res =
		  IRB.CreateInsertValue(UndefValue::get(CASI->getType()), OldVal, 0);
		Res = IRB.CreateInsertValue(Res, Success, 1);

		I->replaceAllUsesWith(Res);
		I->eraseFromParent();
	} else if (FenceInst *FI = dyn_cast<FenceInst>(I)) {
		int atomic_order_index = getAtomicOrderIndex(FI->getOrdering());
		Value *order = ConstantInt::get(OrdTy, atomic_order_index);
		Value *Args[] = {order, position};

		CallInst *funcInst = CallInst::Create(PMCAtomicThreadFence, Args);
		ReplaceInstWithInst(FI, funcInst);
		//errs() << "Thread Fences replaced\n";
	}
	return true;
}



bool PMCPass::isAtomicCall(Instruction *I) {
	if ( auto *CI = dyn_cast<CallInst>(I) ) {
		Function *fun = CI->getCalledFunction();
		if (fun == NULL)
			return false;

		StringRef funName = fun->getName();

		// TODO: come up with better rules for function name checking
		for (StringRef name : AtomicFuncNames) {
			if ( funName.startswith(name) ) 
				return true;
		}
		
		for (StringRef PartialName : PartialAtomicFuncNames) {
			if (funName.contains(PartialName) && 
					funName.contains("atomic") )
				return true;
		}
	}

	return false;
}

bool PMCPass::instrumentAtomicCall(CallInst *CI, const DataLayout &DL) {
	errs() << "Intrumenting Atomic Call: " << *CI << "\n";
	IRBuilder<> IRB(CI);
	Function *fun = CI->getCalledFunction();
	StringRef funName = fun->getName();
	std::vector<Value *> parameters;

	User::op_iterator begin = CI->arg_begin();
	User::op_iterator end = CI->arg_end();
	for (User::op_iterator it = begin; it != end; ++it) {
		Value *param = *it;
		parameters.push_back(param);
	}

	// obtain source line number of the CallInst
	Value *position = getPosition(CI, IRB, true);

	// the pointer to the address is always the first argument
	Value *OrigPtr = parameters[0];

	int Idx = getMemoryAccessFuncIndex(OrigPtr, DL);
	if (Idx < 0)
		return false;

	const unsigned ByteSize = 1U << Idx;
	const unsigned BitSize = ByteSize * 8;
	Type *Ty = Type::getIntNTy(IRB.getContext(), BitSize);
	Type *PtrTy = Ty->getPointerTo();

	// atomic_init; args = {obj, order}
	if (funName.contains("atomic_init")) {
		Value *OrigVal = parameters[1];

		Value *ptr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *val;
		if (OrigVal->getType()->isPtrOrPtrVectorTy())
			val = IRB.CreatePointerCast(OrigVal, Ty);
		else
			val = IRB.CreateIntCast(OrigVal, Ty, true);

		Value *args[] = {ptr, val, position};

		Instruction* funcInst = CallInst::Create(PMCAtomicInit[Idx], args);
		ReplaceInstWithInst(CI, funcInst);

		return true;
	}

	// atomic_load; args = {obj, order}
	if (funName.contains("atomic_load")) {
		bool isExplicit = funName.contains("atomic_load_explicit");

		Value *ptr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *order;
		if (isExplicit)
			order = IRB.CreateBitOrPointerCast(parameters[1], OrdTy);
		else 
			order = ConstantInt::get(OrdTy, 
							(int) AtomicOrderingCABI::seq_cst);
		Value *args[] = {ptr, order, position};
		
		Instruction* funcInst = CallInst::Create(PMCAtomicLoad[Idx], args);
		ReplaceInstWithInst(CI, funcInst);

		return true;
	} else if (funName.contains("atomic") && 
					funName.contains("load") ) {
		// does this version of call always have an atomic order as an argument?
		Value *ptr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *order = IRB.CreateBitOrPointerCast(parameters[1], OrdTy);
		Value *args[] = {ptr, order, position};

		if (!CI->getType()->isPointerTy()) {
			return false;	
		} 

		CallInst *funcInst = IRB.CreateCall(PMCAtomicLoad[Idx], args);
		Value *RetVal = IRB.CreateIntToPtr(funcInst, CI->getType());

		CI->replaceAllUsesWith(RetVal);
		CI->eraseFromParent();

		return true;
	}

	// atomic_store; args = {obj, val, order}
	if (funName.contains("atomic_store")) {
		bool isExplicit = funName.contains("atomic_store_explicit");
		Value *OrigVal = parameters[1];

		Value *ptr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *val = IRB.CreatePointerCast(OrigVal, Ty);
		Value *order;
		if (isExplicit)
			order = IRB.CreateBitOrPointerCast(parameters[2], OrdTy);
		else 
			order = ConstantInt::get(OrdTy, 
							(int) AtomicOrderingCABI::seq_cst);
		Value *args[] = {ptr, val, order, position};
		
		Instruction* funcInst = CallInst::Create(PMCAtomicStore[Idx], args);
		ReplaceInstWithInst(CI, funcInst);

		return true;
	} else if (funName.contains("atomic") && 
					funName.contains("store") ) {
		// Does this version of call always have an atomic order as an argument?
		Value *OrigVal = parameters[1];

		Value *ptr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *val;
		if (OrigVal->getType()->isPtrOrPtrVectorTy())
			val = IRB.CreatePointerCast(OrigVal, Ty);
		else
			val = IRB.CreateIntCast(OrigVal, Ty, true);

		Value *order = IRB.CreateBitOrPointerCast(parameters[2], OrdTy);
		Value *args[] = {ptr, val, order, position};

		Instruction* funcInst = CallInst::Create(PMCAtomicStore[Idx], args);
		ReplaceInstWithInst(CI, funcInst);

		return true;
	}

	// atomic_fetch_*; args = {obj, val, order}
	if (funName.contains("atomic_fetch_") || 
		funName.contains("atomic_exchange")) {

		/* TODO: implement stricter function name checking */
		if (funName.contains("non"))
			return false;

		bool isExplicit = funName.contains("_explicit");
		Value *OrigVal = parameters[1];

		int op;
		if ( funName.contains("_fetch_add") )
			op = AtomicRMWInst::Add;
		else if ( funName.contains("_fetch_sub") )
			op = AtomicRMWInst::Sub;
		else if ( funName.contains("_fetch_and") )
			op = AtomicRMWInst::And;
		else if ( funName.contains("_fetch_or") )
			op = AtomicRMWInst::Or;
		else if ( funName.contains("_fetch_xor") )
			op = AtomicRMWInst::Xor;
		else if ( funName.contains("atomic_exchange") )
			op = AtomicRMWInst::Xchg;
		else {
			errs() << "Unknown atomic read-modify-write operation\n";
			return false;
		}

		Value *ptr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *val;
		if (OrigVal->getType()->isPtrOrPtrVectorTy())
			val = IRB.CreatePointerCast(OrigVal, Ty);
		else
			val = IRB.CreateIntCast(OrigVal, Ty, true);

		Value *order;
		if (isExplicit)
			order = IRB.CreateBitOrPointerCast(parameters[2], OrdTy);
		else 
			order = ConstantInt::get(OrdTy, 
							(int) AtomicOrderingCABI::seq_cst);
		Value *args[] = {ptr, val, order, position};
		
		Instruction* funcInst = CallInst::Create(PMCAtomicRMW[op][Idx], args);
		ReplaceInstWithInst(CI, funcInst);

		return true;
	} else if (funName.contains("fetch")) {
		errs() << "atomic fetch captured. Not implemented yet. ";
		errs() << "See source file :";
		getPosition(CI, IRB, true);
		return false;
	} else if (funName.contains("exchange") &&
			!funName.contains("compare_exchange") ) {
		if (CI->getType()->isPointerTy()) {
			/**
			 * TODO: instrument the following case
			 * mcs-lock.h
			 * std::atomic<struct T *> m_tail;
			 * struct T * me;
			 * struct T * pred = m_tail.exchange(me, memory_order_*);
			 */
			errs() << "atomic exchange captured. Not implemented yet. ";
			errs() << "See source file :";
			getPosition(CI, IRB, true);

			return false;
		}

		Value *OrigVal = parameters[1];

		Value *ptr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *val;
		if (OrigVal->getType()->isPtrOrPtrVectorTy())
			val = IRB.CreatePointerCast(OrigVal, Ty);
		else
			val = IRB.CreateIntCast(OrigVal, Ty, true);

		Value *order = IRB.CreateBitOrPointerCast(parameters[2], OrdTy);
		Value *args[] = {ptr, val, order, position};
		int op = AtomicRMWInst::Xchg;

		Instruction* funcInst = CallInst::Create(PMCAtomicRMW[op][Idx], args);
		ReplaceInstWithInst(CI, funcInst);

		return true;
	}

	/* atomic_compare_exchange_*; 
	   args = {obj, expected, new value, order1, order2}
	*/
	if ( funName.contains("atomic_compare_exchange_") ) {
		bool isExplicit = funName.contains("_explicit");

		Value *Addr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *CmpOperand = IRB.CreatePointerCast(parameters[1], PtrTy);
		Value *NewOperand = IRB.CreateBitOrPointerCast(parameters[2], Ty);

		Value *order_succ, *order_fail;
		if (isExplicit) {
			order_succ = IRB.CreateBitOrPointerCast(parameters[3], OrdTy);

			if (parameters.size() > 4) {
				order_fail = IRB.CreateBitOrPointerCast(parameters[4], OrdTy);
			} else {
				/* The failure order is not provided */
				order_fail = order_succ;
				ConstantInt * order_succ_cast = dyn_cast<ConstantInt>(order_succ);
				int index = order_succ_cast->getSExtValue();

				order_fail = ConstantInt::get(OrdTy,
								AtomicCasFailureOrderIndex(index));
			}
		} else  {
			order_succ = ConstantInt::get(OrdTy, 
							(int) AtomicOrderingCABI::seq_cst);
			order_fail = ConstantInt::get(OrdTy, 
							(int) AtomicOrderingCABI::seq_cst);
		}

		Value *args[] = {Addr, CmpOperand, NewOperand, 
							order_succ, order_fail, position};
		
		Instruction* funcInst = CallInst::Create(PMCAtomicCAS_V2[Idx], args);
		ReplaceInstWithInst(CI, funcInst);

		return true;
	} else if ( funName.contains("compare_exchange_strong") ||
				funName.contains("compare_exchange_weak") ) {
		Value *Addr = IRB.CreatePointerCast(OrigPtr, PtrTy);
		Value *CmpOperand = IRB.CreatePointerCast(parameters[1], PtrTy);
		Value *NewOperand = IRB.CreateBitOrPointerCast(parameters[2], Ty);

		Value *order_succ, *order_fail;
		order_succ = IRB.CreateBitOrPointerCast(parameters[3], OrdTy);

		if (parameters.size() > 4) {
			order_fail = IRB.CreateBitOrPointerCast(parameters[4], OrdTy);
		} else {
			/* The failure order is not provided */
			order_fail = order_succ;
			ConstantInt * order_succ_cast = dyn_cast<ConstantInt>(order_succ);
			int index = order_succ_cast->getSExtValue();

			order_fail = ConstantInt::get(OrdTy,
							AtomicCasFailureOrderIndex(index));
		}

		Value *args[] = {Addr, CmpOperand, NewOperand, 
							order_succ, order_fail, position};
		Instruction* funcInst = CallInst::Create(PMCAtomicCAS_V2[Idx], args);
		ReplaceInstWithInst(CI, funcInst);

		return true;
	}

	return false;
}
#endif
int PMCPass::getMemoryAccessFuncIndex(Value *Addr,
										const DataLayout &DL) {
	Type *OrigPtrTy = Addr->getType();
	Type *OrigTy = cast<PointerType>(OrigPtrTy)->getElementType();
	assert(OrigTy->isSized());
	uint32_t TypeSize = DL.getTypeStoreSizeInBits(OrigTy);
	if (TypeSize != 8  && TypeSize != 16 &&
		TypeSize != 32 && TypeSize != 64 && TypeSize != 128) {
		NumAccessesWithBadSize++;
		// Ignore all unusual sizes.
		return -1;
	}
	size_t Idx = countTrailingZeros(TypeSize / 8);
	//assert(Idx < kNumberOfAccessSizes);
	if (Idx >= kNumberOfAccessSizes) {
		return -1;
	}
	return Idx;
}


char PMCPass::ID = 0;

// Automatically enable the pass.
static void registerPMCPass(const PassManagerBuilder &,
							legacy::PassManagerBase &PM) {
	PM.add(new PMCPass());
}

/* Enable the pass when opt level is greater than 0 */
static RegisterStandardPasses 
	RegisterMyPass1(PassManagerBuilder::EP_OptimizerLast,
registerPMCPass);

/* Enable the pass when opt level is 0 */
static RegisterStandardPasses 
	RegisterMyPass2(PassManagerBuilder::EP_EnabledOnOptLevel0,
registerPMCPass);

