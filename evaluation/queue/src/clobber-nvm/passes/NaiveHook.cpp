#include "Common.hpp"
#include "MemoryIdempotenceAnalysis.h"


//===----------------------------------------------------------------------===//
//
// This file contains the implementation for instrument nvm-related callbacks
// at the LLVM IR level.
// The implementation is based on the code provided in (unpublished)
// "NVHooks: A Flexible, Optimizing Compiler for Non-Volatile Memory Programming".
// See "Clobber-NVM: Log Less, Re-execute More" in ASPLOS'21
//
//
//===----------------------------------------------------------------------===//



static cl::opt<bool> RangeCheck("range-check",
        cl::desc("Invoke `is_nvmm(...)` prior tracking/swizzling accesses"),
        cl::value_desc("Enable by default"));
static cl::opt<bool> StaticRangeCheck("static-range-check",
        cl::desc("Replace `is_nvmm()` with static range-checking: any address above 2^48 is tracked."),
        cl::value_desc("Disabled by default"));
static cl::opt<bool> StoreTracking("store-tracking",
        cl::desc("Instrument stores with `on_nvmm_write(...)` to track writes"),
        cl::value_desc("Enable by default"));
static cl::opt<bool> LoadTracking("load-tracking",
        cl::desc("Instrument loads with `on_nvmm_read(...)` to track reads"),
        cl::value_desc("Disabled by default"));
static cl::opt<bool> PointerSwizzling("pointer-swizzling",
        cl::desc("Invoke `to_absolute_ptr(...)` prior load/store to swizzle pointers"),
        cl::value_desc("Disabled by default"));
static cl::opt<bool> PostStoreCallback("post-store-callback",
        cl::desc("Follow stores to NVMM with a callback to `post_nvmm_store(...)`"),
        cl::value_desc("Disabled by default"));
static cl::opt<bool> PostLoadCallback("post-load-callback",
        cl::desc("Follow loads to NVMM with a callback to `post_nvmm_load(...)`"),
        cl::value_desc("Disabled by default"));

namespace {
    class MemoryAccess {
    public:
        Instruction *inst;
        Value *ptr;
        Type *type;
        Value *size;
        // Instructions to move to the NVM basic block
        std::vector<Instruction *> nvmBlock;
        // Pointer to the element for tracking by the NVM tracker function
        Value *elementPtr;

        MemoryAccess(Instruction *i, Value *p, Type *t, Value *sz = NULL, Value *ep = NULL) :
            inst(i), ptr(p), type(t), size(sz), elementPtr(ep) {
                if (sz == NULL) {
                    Module *m = inst->getModule();
                    DataLayout *layout = new DataLayout(m);
                    size = ConstantInt::get(
                            IntegerType::get(m->getContext(), 64),
                            layout->getTypeAllocSize(type), false);
                }
            }
    };

    class Store : public MemoryAccess {
    public:
        Store(Instruction *i, Value *p, Type *t, Value *sz = NULL, Value *ep = NULL) :
            MemoryAccess(i, p, t, sz, ep) {}
    };

    class Load : public MemoryAccess {
    public:
        Load(Instruction *i, Value *p, Type *t, Value *sz = NULL, Value *ep = NULL) :
            MemoryAccess(i, p, t, sz, ep) {}
    };

    class NaiveHook : public FunctionPass {
    public:
        static char ID;
        NaiveHook() : FunctionPass(ID) { }

        bool doInitialization(Module &M) {
            if (RangeCheck.getNumOccurrences() > 0) {
                enableRangeCheck = RangeCheck;
                outs() << "Range checking is " << enableRangeCheck << "\n";
            }
            if (StaticRangeCheck.getNumOccurrences() > 0) {
                enableStaticRangeCheck = StaticRangeCheck;
                outs() << "Static range checking is " << enableStaticRangeCheck << "\n";
            }
            if (StoreTracking.getNumOccurrences() > 0) {
                enableWriteTracking = StoreTracking;
                outs() << "Write tracking is " << enableWriteTracking << "\n";
            }
            if (LoadTracking.getNumOccurrences() > 0) {
                enableReadTracking = LoadTracking;
                outs() << "Load tracking is " << enableReadTracking << "\n";
            }
            if (PointerSwizzling.getNumOccurrences() > 0) {
                enableSwizzling = PointerSwizzling;
                outs() << "Swizzling is " << enableSwizzling << "\n";
            }
            if (PostStoreCallback.getNumOccurrences() > 0) {
                enablePostWriteCallback = PostStoreCallback;
                outs() << "Post-store callback is " << enablePostWriteCallback << "\n";
            }
            if (PostLoadCallback.getNumOccurrences() > 0) {
                enablePostReadCallback = PostLoadCallback;
                outs() << "Post-load callback is " << enablePostReadCallback << "\n";
            }

            assert(!enableStaticRangeCheck || enableRangeCheck);
            assert(enableRangeCheck || !enablePostWriteCallback);
            assert(enableRangeCheck || !enablePostReadCallback);
            assert(enableRangeCheck || !enableSwizzling);

            if (enableStaticRangeCheck) return false;

            FunctionType *funcType = TypeBuilder<int(void *), false>::get(M.getContext());
            Function *func = (Function*)M.getOrInsertFunction(rangeCheckFunction, funcType);
            func->addFnAttr(Attribute::ReadOnly);
            func->addFnAttr(Attribute::InlineHint);
            func->addFnAttr(Attribute::NonLazyBind);
            func->addFnAttr(Attribute::Speculatable);

            return true;
        }

        /*
         * helper (utility) methods
         */

        // TODO implement nvmm_strlen here
        CallInst *createStrLen(Instruction *inst, Value *str, bool swizzled = false) {
            std::string strLenFunc = "strlen";
            if (enableSwizzling && !swizzled) strLenFunc = nvmmStrLen;

            Module *m = inst->getModule();
            FunctionType *funcType =
                TypeBuilder<size_t(void *), false>::get(m->getContext());
            Function *strLen =
                cast<Function>(m->getOrInsertFunction(strLenFunc, funcType));
            std::vector<Value *> varg_list;
            varg_list.push_back(str);
            CallInst *strlenInst = CallInst::Create(strLen, varg_list, "strlen", inst);
            strlenInst->setDebugLoc(inst->getDebugLoc());
            return strlenInst;
        }

        template <class T>
        Instruction *swizzlePointer(Instruction *inst, Value *addr,
                Instruction *nvmInst, Instruction *checkInst, T *access,
                AllocaInst *swizzlingBuffer) {
            assert(nvmInst != NULL);
            assert(checkInst != NULL);

            Module *m = static_cast<Module *>(inst->getModule());
            LLVMContext &context = inst->getContext();
            IntegerType *intType = IntegerType::get(context, 64);
            PointerType *ptrType = Type::getIntNPtrTy(context, 8); // 64?

            // first instruction in the NVM basic-block
            Instruction *first = nvmInst->getParent()->getFirstNonPHIOrDbg();

            // buffer for the pointer address (i.e., swizzlingBuffer = addr)
            PtrToIntInst *intAddr = new PtrToIntInst(addr, intType, "", checkInst);
            StoreInst *storeOrigAddr = new StoreInst(intAddr, swizzlingBuffer, checkInst);
            storeOrigAddr->setDebugLoc(inst->getDebugLoc());

            // update the buffer with the swizzled pointer
            FunctionType *funcType = TypeBuilder<void *(void *), false>::get(context);
            Function *func =
                cast<Function>(m->getOrInsertFunction(swizzlingFunction, funcType));

            std::vector<Value *> varg_list;
            varg_list.push_back(addr);

            Instruction *swizzling = CallInst::Create(func, varg_list, "", first);
            swizzling->setDebugLoc(inst->getDebugLoc());
            PtrToIntInst *intSwizzled = new PtrToIntInst(swizzling, intType, "", first);
            StoreInst *storeNewAddr = new StoreInst(intSwizzled, swizzlingBuffer, first);
            storeNewAddr->setDebugLoc(inst->getDebugLoc());

            // update the store instruction to use the address in buffer
            LoadInst *buffer = new LoadInst(swizzlingBuffer, "", inst);
            buffer->setDebugLoc(inst->getDebugLoc());
            IntToPtrInst *bufferPtr = new IntToPtrInst(buffer, ptrType, "", inst);
            Value *newPtr = cast<Value>(new BitCastInst(bufferPtr,
                        access->ptr->getType(), "", access->inst));
            for (auto i = inst->op_begin(); i != inst->op_end(); ++i) {
                Value *val = *i;
                if (val == access->ptr) *i = newPtr;
            }

            return swizzling; // callback to `to_absolute_ptr(...)`
        }

        Instruction *createAddInst(Instruction *next, Value *v1, Value *v2) {
            return BinaryOperator::Create(Instruction::Add, v1, v2, "", next);
        }

        Instruction *createIncrement(Instruction *next, Value *val) {
            LLVMContext &context = next->getModule()->getContext();
            ConstantInt *constOne =
                ConstantInt::get(IntegerType::get(context, 64), 1, false);
            return createAddInst(next, val, constOne);
        }

        virtual bool isOnStack(Instruction *inst, Value *value) {
            if (isa<ConstantExpr>(value)){
		 //errs()<<"stack value, return true \n";
		 return true;
	    }

            if (isa<SelectInst>(value)) {
                Value* v1 = cast<SelectInst>(value)->getTrueValue();
                Value* v2 = cast<SelectInst>(value)->getFalseValue();
                return isOnStack(inst, v1) && isOnStack(inst, v2);
            }
            if (isa<BitCastInst>(value)) {
                value = cast<BitCastInst>(value)->getOperand(0);
            }
            if (isa<GetElementPtrInst>(value)) {
                value = cast<GetElementPtrInst>(value)->getOperand(0);
            }
            if (isa<AllocaInst>(value)){
		 //errs()<<"alloc inst, return true \n";
		 return true;
	    }
            // TODO more logic to filter-out read/write accesses to stack
	    //errs()<<"not a stack value, return false \n";
            return false;
        }

        template <class T>
        bool enablePostAccessCallback() {
            if (std::is_same<T, Store>::value && enablePostWriteCallback) return true;
            if (std::is_same<T, Load>::value && enablePostReadCallback) return true;
            return false;
        }

        template <class T>
        Instruction *createRangeTrack(Instruction *inst,
                Value *addr, Value *size) {
            std::string trackerFunction = writeTrackFunction;
            if (std::is_same<T, Load>::value) trackerFunction = readTrackFunction;

            Module *m = static_cast<Module *>(inst->getModule());
            FunctionType *funcType =
                TypeBuilder<void(void *, size_t), false>::get(m->getContext());
            Function *func = (Function*)m->getOrInsertFunction(trackerFunction, funcType);

            std::vector<Value *> varg_list;
            varg_list.push_back(addr);
            varg_list.push_back(size);

            CallInst *rangeTrackCall = CallInst::Create(func, varg_list, "", inst);
            rangeTrackCall->setDebugLoc(inst->getDebugLoc());
            return rangeTrackCall;
        }



	template <class T>
        Instruction *createRAWRangeTrack(Instruction *inst,
                Value *addr, Value *size) {
            std::string trackerFunction = RAWwriteTrackFunction;

            Module *m = static_cast<Module *>(inst->getModule());
            FunctionType *funcType =
                TypeBuilder<void(void *, size_t), false>::get(m->getContext());
            Function *func = (Function*)m->getOrInsertFunction(trackerFunction, funcType);

            std::vector<Value *> varg_list;
            varg_list.push_back(addr);
            varg_list.push_back(size);

            CallInst *rangeTrackCall = CallInst::Create(func, varg_list, "", inst);
            rangeTrackCall->setDebugLoc(inst->getDebugLoc());
            return rangeTrackCall;
        }




        Instruction *rangeCheckCallback(Instruction *inst, Value *addr,
                Instruction *nvmInst) {
            Module *m = static_cast<Module *>(inst->getModule());

            FunctionType *funcType =
                TypeBuilder<int(void *), false>::get(m->getContext());
            Function *func =
                (Function*)m->getOrInsertFunction(rangeCheckFunction, funcType);

            std::vector<Value *> varg_list;
            varg_list.push_back(addr);

            CallInst *rangeCheckCall = CallInst::Create(func, varg_list, "", nvmInst);
            rangeCheckCall->setDebugLoc(inst->getDebugLoc());
            return rangeCheckCall;
        }

        Instruction *staticRangeCheck(Instruction *inst, Value *addr,
                Instruction *nvmInst) {
            LLVMContext &context = inst->getContext();
            IntegerType *intType = IntegerType::get(context, 64);
            PtrToIntInst *intPtr = new PtrToIntInst(addr, intType, "", nvmInst);
            ConstantInt *nvmLimit = ConstantInt::get(intType, 0x1000000000000, false);
            return CmpInst::Create(Instruction::ICmp, CmpInst::ICMP_UGE,
                    cast<Value>(intPtr), cast<Value>(nvmLimit), "", nvmInst);
        }

        virtual Instruction *createRangeCheck(Instruction *inst, Value *addr,
                Instruction *nvmInst) {
            assert(nvmInst != NULL);
            LLVMContext &context = inst->getContext();

            Instruction *check = NULL;
            CmpInst *cmp = NULL;

            if (!enableStaticRangeCheck) {
                check = rangeCheckCallback(inst, addr, nvmInst);
                IntegerType *intType = IntegerType::get(context, 32);
                ConstantInt *constOne = ConstantInt::get(intType, 1, true);
                cmp = CmpInst::Create(Instruction::ICmp, CmpInst::ICMP_EQ,
                        check, cast<Value>(constOne), "", nvmInst);
            }
            else {
                check = staticRangeCheck(inst, addr, nvmInst);
                cmp = cast<CmpInst>(check);
            }

            cmp->setDebugLoc(inst->getDebugLoc());

            // branching to handle both NVMM and non-NVMM accesses
            BasicBlock *bbTrue = nvmInst->getParent()->splitBasicBlock(nvmInst);
            BasicBlock *bbFalse = inst->getParent()->splitBasicBlock(inst);

            cmp->getParent()->getTerminator()->eraseFromParent();
            BranchInst *b = BranchInst::Create(bbTrue, bbFalse, cmp, cmp->getParent());
            b->setDebugLoc(inst->getDebugLoc());

            MDBuilder MDB(context);
            b->setMetadata(LLVMContext::MD_prof, MDB.createBranchWeights(0, UINT32_MAX));

            return check;
        }

        template <class T>
        Instruction *createPostAccessCallback(Instruction *inst,
                Instruction *check, Instruction *track,
                AllocaInst *addrBuffer, AllocaInst *sizeBuffer) {
            Module *m = static_cast<Module *>(inst->getModule());
            LLVMContext &context = inst->getContext();
            IntegerType *intType = IntegerType::get(context, 64);
            PointerType *ptrType = Type::getIntNPtrTy(context, 8); // 64?

            BasicBlock *nvmBB = track->getParent();
            CmpInst *cmp = enableStaticRangeCheck ?
                cast<CmpInst>(check) :
                cast<CmpInst>(check->getNextNonDebugInstruction());
            //cmp->setDebugLoc(inst->getDebugLoc());

            CallInst *nvmTrack = cast<CallInst>(track);
            assert(nvmTrack->getNumArgOperands() == 2);
            Value *addr = nvmTrack->getArgOperand(0);
            Value *size = nvmTrack->getArgOperand(1);

            Instruction *nvmLast = nvmBB->getTerminator();
            PtrToIntInst *intAddr = new PtrToIntInst(addr, intType, "", nvmLast);
            new StoreInst(intAddr, addrBuffer, nvmLast);
            new StoreInst(size, sizeBuffer, nvmLast);

            // Create the post-access callback
            std::string funcName = postLoadFunction;
            if (std::is_same<T, Store>::value) funcName = postStoreFunction;
            Instruction *next = inst->getNextNonDebugInstruction();
            assert(next != NULL);
            LoadInst *trackedAddr = new LoadInst(addrBuffer, "", next);
            addr = new IntToPtrInst(trackedAddr, ptrType, "", next);
            size = new LoadInst(sizeBuffer, "", next);
            FunctionType *funcType = TypeBuilder<void(void*,size_t), false>::get(context);
            Function *func = cast<Function>(m->getOrInsertFunction(funcName, funcType));
            std::vector<Value *> varg_list;
            varg_list.push_back(addr);
            varg_list.push_back(size);
            Instruction *callback = CallInst::Create(func, varg_list, "", next);
            callback->setDebugLoc(inst->getDebugLoc());

            // Create the branch to skip callback for non-NVMM accesses
            BasicBlock *bbTrue = callback->getParent()->splitBasicBlock(trackedAddr);
            BasicBlock *bbFalse = next->getParent()->splitBasicBlock(next);
            inst->getParent()->getTerminator()->eraseFromParent();
            BranchInst *b = BranchInst::Create(bbTrue, bbFalse, cmp, inst->getParent());
            b->setDebugLoc(inst->getDebugLoc());

            MDBuilder MDB(context);
            b->setMetadata(LLVMContext::MD_prof, MDB.createBranchWeights(0, UINT32_MAX));

            return callback;
        }




	void collectStoreFromAntidependencyPairs(std::vector<Store *> &stores){
            MemoryIdempotenceAnalysis *MIA = &getAnalysis<MemoryIdempotenceAnalysis>();
            for (MemoryIdempotenceAnalysis::const_iterator I = MIA->begin(),
            E = MIA->end(); I != E; ++I) {

                Instruction *LI;// = &*Load;
                Instruction *SI;// = &*Store;
                tie(LI, SI) = *I;

                if(StoreInst *i = dyn_cast<StoreInst>(SI))
                stores.push_back(new Store(i, i->getPointerOperand(),
                            i->getValueOperand()->getType()));

            }
        }



	template <class T>
	Instruction* getInstFromAccess(T *access){
	    Instruction *inst = access->inst;
	    return inst;
	}




	template <class T>
        Value* getPtrFromAccess(T *access){
            Value *ptr = access->ptr;
            return ptr;
        }


        /*
         * Callback function for every load/store
         */

        template <class T>
        bool runOnAccess(T *access, AllocaInst *swizzlingBuffer,
                AllocaInst *postCallbackBuffers[2]) {
            if (isOnStack(access->inst, access->ptr)) return false;

            LLVMContext &context = access->inst->getContext();
            PointerType *ptrType = Type::getIntNPtrTy(context, 8); // 64?

            Instruction *inst = access->inst;
            Value *addr = access->ptr;
            Value *size = access->size;

            if (addr->getType() != ptrType) {
                addr = new BitCastInst(addr, ptrType, "", inst);
            }


	    Instruction *accessTrack = NULL;
	   /* if (isOnStack(access->inst, access->ptr))
		accessTrack = createRAWRangeTrack<T>(inst,
                    access->elementPtr == NULL ? addr : access->elementPtr, size);
            // elementPtr points to a region in a larger memory block (e.g., strcat)
            else
	
	    */
	    accessTrack = createRangeTrack<T>(inst,
                    access->elementPtr == NULL ? addr : access->elementPtr, size);

	    //Instruction *accessTrack = inst;

            Instruction *check = NULL;
            if (enableRangeCheck) {
                check = createRangeCheck(inst, addr, accessTrack);
                if (enableSwizzling) {
                    Instruction *swizzled = swizzlePointer(inst, addr,
                            accessTrack, check, access, swizzlingBuffer);

                    // use the swizzled pointer in the NVM basic block
                    Instruction *i = swizzled->getNextNonDebugInstruction();
                    while (i != NULL) {
                        for (auto j = i->op_begin(); j != i->op_end(); ++j) {
                            Value *val = *j;
                            if (val == addr) *j = swizzled;
                        }
                        i = i->getNextNonDebugInstruction();
                    }

                    for (size_t i = 0; i < access->nvmBlock.size(); i++) {
                        Instruction *inst = access->nvmBlock[i];
                        for (auto j = inst->op_begin(); j != inst->op_end(); ++j) {
                            Value *val = *j;
                            if (val == access->ptr) *j = swizzled;
                        }
                    } // NVM block instructions

                } // pointer swizzling
            } // range check


	    
            for (size_t i = 0; i < access->nvmBlock.size(); i++) {
                Instruction *inst = access->nvmBlock[i];
                inst->moveBefore(accessTrack);
            }
            access->nvmBlock.clear();

	     
            // Move the NVM basic block to the end
            if (enableRangeCheck) {
                BasicBlock &lastBB = accessTrack->getParent()->getParent()->back();
                accessTrack->getParent()->moveAfter(&lastBB);
            }

            return true;
        }



	template <class T>
        bool runOnRAWAccess(T *access, AllocaInst *swizzlingBuffer,
                AllocaInst *postCallbackBuffers[2]) {
            if (isOnStack(access->inst, access->ptr)) return false;

            LLVMContext &context = access->inst->getContext();
            PointerType *ptrType = Type::getIntNPtrTy(context, 8); // 64?

            Instruction *inst = access->inst;
            Value *addr = access->ptr;
            Value *size = access->size;

            if (addr->getType() != ptrType) {
                addr = new BitCastInst(addr, ptrType, "", inst);
            }

            // elementPtr points to a region in a larger memory block (e.g., strcat)
            Instruction *accessTrack = createRAWRangeTrack<T>(inst,
                    access->elementPtr == NULL ? addr : access->elementPtr, size);

            //Instruction *accessTrack = inst;

            Instruction *check = NULL;
            if (enableRangeCheck) {
                check = createRangeCheck(inst, addr, accessTrack);
                if (enableSwizzling) {
                    Instruction *swizzled = swizzlePointer(inst, addr,
                            accessTrack, check, access, swizzlingBuffer);

                    // use the swizzled pointer in the NVM basic block
                    Instruction *i = swizzled->getNextNonDebugInstruction();


		    while (i != NULL) {
                        for (auto j = i->op_begin(); j != i->op_end(); ++j) {
                            Value *val = *j;
                            if (val == addr) *j = swizzled;
                        }
                        i = i->getNextNonDebugInstruction();
                    }

                    for (size_t i = 0; i < access->nvmBlock.size(); i++) {
                        Instruction *inst = access->nvmBlock[i];
                        for (auto j = inst->op_begin(); j != inst->op_end(); ++j) {
                            Value *val = *j;
                            if (val == access->ptr) *j = swizzled;
                        }
                    } // NVM block instructions

                } // pointer swizzling
            } // range check



            for (size_t i = 0; i < access->nvmBlock.size(); i++) {
                Instruction *inst = access->nvmBlock[i];
                inst->moveBefore(accessTrack);
            }
            access->nvmBlock.clear();


            // Move the NVM basic block to the end
            if (enableRangeCheck) {
                BasicBlock &lastBB = accessTrack->getParent()->getParent()->back();
                accessTrack->getParent()->moveAfter(&lastBB);
            }

            return true;
    }



        /*
         * collection and initialization methods
         */

        void collectStore(Instruction *inst, std::vector<Store *> &stores) {
            if (isa<StoreInst>(inst)) {
                StoreInst *i = cast<StoreInst>(inst);
                stores.push_back(new Store(i, i->getPointerOperand(),
                            i->getValueOperand()->getType()));
            }
            else if (isa<MemSetInst>(inst) || isa<MemTransferInst>(inst)) {
                CallInst *i = cast<CallInst>(inst);
                assert(i->getNumArgOperands() >= 3);
                stores.push_back(new Store(i, i->getArgOperand(0),
                            i->getArgOperand(1)->getType(), i->getArgOperand(2)));
            }
            else if (isa<MemCpyInst>(inst)) {
                MemCpyInst *i = cast<MemCpyInst>(inst);
                assert(i->getNumArgOperands() == 3);
                assert(isa<PointerType>(i->getArgOperand(0)->getType()));
                PointerType *ptrType = cast<PointerType>(i->getArgOperand(0)->getType());
                stores.push_back(new Store(i, i->getArgOperand(0),
                            ptrType->getElementType(), i->getArgOperand(2)));
            }
            else if (isa<CallInst>(inst)) {
                // Atomic intrinsics are not supported
                assert(!isa<AtomicMemCpyInst>(inst));
                assert(!isa<AtomicMemSetInst>(inst));
                assert(!isa<AtomicMemTransferInst>(inst));

                CallInst *call = cast<CallInst>(inst);
                if (call->isTailCall()) return;
                if (call->isInlineAsm()) {
                    std::string str;
                    llvm::raw_string_ostream rso(str);
                    inst->print(rso);
                    errs() << "NaiveHook does not support inline assembly: ";
                    errs() << str << "\n";
                    return;
                }

                Function *func = call->getCalledFunction();
                if (func == NULL) return;

                if (func->getName() == "strcpy" || func->getName() == "strncpy") {
                    assert(call->getNumArgOperands() >= 2);
                    PointerType *ptrType =
                        cast<PointerType>(call->getArgOperand(0)->getType());
                    if (isOnStack(call, call->getArgOperand(0))) return;
                    Value *size = NULL;
                    if (func->getName() == "strcpy") {
                        size = createStrLen(call, call->getArgOperand(1));
                    }
                    else { // strncpy
                        assert(call->getNumArgOperands() == 3);
                        size = call->getArgOperand(2);
                    }
                    Instruction *sz = createIncrement(call, size);

                    Store *s = new Store(inst, call->getArgOperand(0),
                            ptrType->getElementType(), sz);

                    if (func->getName() == "strcpy")
                        s->nvmBlock.push_back(cast<Instruction>(size));
                    s->nvmBlock.push_back(sz);
                    stores.push_back(s);
                }
                else if (func->getName() == "strcat" || func->getName() == "strncat") {
                    assert(call->getNumArgOperands() >= 2);
                    if (isOnStack(call, call->getArgOperand(0))) return;

                    LLVMContext &context = inst->getModule()->getContext();
                    PointerType *ptrType =
                        cast<PointerType>(call->getArgOperand(0)->getType());
                    IntegerType *intType = IntegerType::get(context, 64);

                    // Source length
                    Value *szMinusOne = NULL;
                    if (func->getName() == "strcat") {
                        szMinusOne = createStrLen(call, call->getArgOperand(1));
                    }
                    else { // strncat
                        assert(call->getNumArgOperands() == 3);
                        szMinusOne = call->getArgOperand(2);
                    }
                    Value *size = createIncrement(call, szMinusOne);

                    // Destination pointer (calculate elementPtr)
                    Value *strLen = createStrLen(call, call->getArgOperand(0), true);
                    PtrToIntInst *t1 = new PtrToIntInst(call->getArgOperand(0),
                            intType, "", call);
                    Instruction *t2 = createAddInst(call, t1, strLen);
                    IntToPtrInst *elementPtr = new IntToPtrInst(t2, ptrType, "", call);

                    Store *s = new Store(inst,
                            call->getArgOperand(0),
                            ptrType->getElementType(),
                            size,
                            elementPtr);
                    if (func->getName() == "strcat")
                        s->nvmBlock.push_back(cast<Instruction>(szMinusOne));
                    s->nvmBlock.push_back(cast<Instruction>(size));
                    s->nvmBlock.push_back(cast<Instruction>(strLen));
                    s->nvmBlock.push_back(t1);
                    s->nvmBlock.push_back(t2);
                    s->nvmBlock.push_back(elementPtr);
                    stores.push_back(s);
                }
            }
            else if (isa<AtomicCmpXchgInst>(inst)) {
                AtomicCmpXchgInst *i = cast<AtomicCmpXchgInst>(inst);
                stores.push_back(new Store(i, i->getPointerOperand(),
                            i->getNewValOperand()->getType()));
            }
            else if (isa<AtomicRMWInst>(inst)) {
                AtomicRMWInst *i = cast<AtomicRMWInst>(inst);
                stores.push_back(new Store(i, i->getPointerOperand(),
                            i->getValOperand()->getType()));
            }
        }

        void collectLoad(Instruction *inst, std::vector<Load *> &loads) {
            if (isa<LoadInst>(inst)) {
                LoadInst *i = cast<LoadInst>(inst);
                if (i->getPointerOperand()->getName() == "stderr" ||
                        i->getPointerOperand()->getName() == "stdout") return;
                loads.push_back(new Load(i, i->getPointerOperand(),
                            i->getPointerOperandType()));
            } else if (isa<MemTransferInst>(inst)) { // TODO make sure we skip memset
                CallInst *i = cast<CallInst>(inst);
                assert(i->getNumArgOperands() >= 3);
                loads.push_back(new Load(i, i->getArgOperand(1),
                            i->getArgOperand(1)->getType(), i->getArgOperand(2)));
            } else if (isa<MemCpyInst>(inst)) {
                MemCpyInst *i = cast<MemCpyInst>(inst);
                assert(i->getNumArgOperands() == 3);
                assert(isa<PointerType>(i->getArgOperand(1)->getType()));
                PointerType *ptrType = cast<PointerType>(i->getArgOperand(1)->getType());
                loads.push_back(new Load(i, i->getArgOperand(1),
                            ptrType->getElementType(), i->getArgOperand(2)));
            } else if (isa<CallInst>(inst)) {
                // Atomic intrinsics are not supported
                assert(!isa<AtomicMemCpyInst>(inst));
                assert(!isa<AtomicMemTransferInst>(inst));

                CallInst *call = cast<CallInst>(inst);
                if (call->isTailCall()) return;
                if (call->isInlineAsm()) {
                    std::string str;
                    llvm::raw_string_ostream rso(str);
                    inst->print(rso);
                    errs() << "NaiveHook does not support inline assembly: ";
                    errs() << str << "\n";
                    return;
                }

                Function *func = call->getCalledFunction();
                if (func == NULL) return;

                // TODO replace strlen() with the output of nvmm_strlen()
                if (func->getName() == "strlen") {
                    assert(call->getNumArgOperands() == 1);
                    if (isOnStack(call, call->getArgOperand(0))) return;
                    PointerType *ptrType =
                        cast<PointerType>(call->getArgOperand(0)->getType());
                    Value *strLength = createStrLen(call, call->getArgOperand(0), true);
                    Instruction *size = createIncrement(call, strLength);
                    Load *l = new Load(inst, call->getArgOperand(0), ptrType->getElementType(), size);
                    l->nvmBlock.push_back(cast<Instruction>(strLength));
                    l->nvmBlock.push_back(size);
                    loads.push_back(l);
                }
                // TODO embed binary for the nvmm counterparts in the code file
                else if (func->getName() == "strcmp" || func->getName() == "memcmp") {
                    assert(call->getNumArgOperands() >= 2);
                    // we replace called functions with nvmm-safe versions
                    Module *module = inst->getModule();
                    FunctionType *funcType = func->getFunctionType();
                    std::string funcName =
                        func->getName() == "strcmp" ? nvmmStrCmp : nvmmMemCmp;
                    Function *nvmmSafeCounterpart =
                        cast<Function>(module->getOrInsertFunction(funcName, funcType));
                    call->setCalledFunction(nvmmSafeCounterpart);
                    // nothing more to do
                }
                else if (func->getName() == "strcpy" || func->getName() == "strncpy") {
                    assert(call->getNumArgOperands() >= 2);
                    PointerType *ptrType =
                        cast<PointerType>(call->getArgOperand(1)->getType());
                    if (isOnStack(call, call->getArgOperand(1))) return;
                    Value *size = NULL;
                    if (func->getName() == "strcpy") {
                        size = createStrLen(call, call->getArgOperand(1), true);
                    } else { // strncpy
                        assert(call->getNumArgOperands() == 3);
                        size = call->getArgOperand(2);
                    }
                    Instruction *sz = createIncrement(call, size);

                    Load *l = new Load(inst, call->getArgOperand(1),
                            ptrType->getElementType(), sz);

                    if (func->getName() == "strcpy")
                        l->nvmBlock.push_back(cast<Instruction>(size));
                    l->nvmBlock.push_back(sz);
                    loads.push_back(l);
                } else if (func->getName() == "strcat" || func->getName() == "strncat") {
                    assert(call->getNumArgOperands() >= 2);
                    PointerType *ptrType =
                        cast<PointerType>(call->getArgOperand(1)->getType());
                    if (isOnStack(call, call->getArgOperand(1))) return;

                    Value *szMinusOne = NULL;
                    if (func->getName() == "strcat") {
                        szMinusOne = createStrLen(call, call->getArgOperand(1), true);
                    } else { // strncat
                        assert(call->getNumArgOperands() == 3);
                        szMinusOne = call->getArgOperand(2);
                    }
                    Value *size = createIncrement(call, szMinusOne);

                    Load *l = new Load(inst, call->getArgOperand(1),
                            ptrType->getElementType(), size);
                    if (func->getName() == "strcat")
                        l->nvmBlock.push_back(cast<Instruction>(szMinusOne));
                    l->nvmBlock.push_back(cast<Instruction>(size));
                    loads.push_back(l);
                }
            }
        }

        bool isHooksFunction(Function *func) {
            if (func->getName() == rangeCheckFunction ||
                    func->getName() == writeTrackFunction ||
                    func->getName() == readTrackFunction ||
                    func->getName() == swizzlingFunction ||
                    func->getName() == postStoreFunction ||
                    func->getName() == postLoadFunction ||
                    func->getName() == nvmmStrLen ||
                    func->getName() == nvmmStrCmp ||
                    func->getName() == nvmmMemCmp) return true;
            else return false;
        }

        bool isHooksCallback(Instruction *inst) {
            if (!isa<CallInst>(inst)) return false;
            CallInst *call = cast<CallInst>(inst);
            if (call->isTailCall() || call->isInlineAsm()) return false;

            Function *func = call->getCalledFunction();
            if (func == NULL) return false;
            return isHooksFunction(func);
        }

        bool runOnFunction(Function &F) override {
            bool retVal = false;
            std::vector<Store *> stores;
            std::vector<Load *> loads;

            if (isHooksFunction(&F)) return false;
            // collect interesting instructions (loads, stores, and library calls)
            for (BBIterator bi = F.getBasicBlockList().begin();
                    bi != F.getBasicBlockList().end(); bi++) {
                BasicBlock *bb = &(*bi);
                for (InstIterator ii = bb->begin(); ii != bb->end(); ii++) {
                    Instruction *inst = &(*ii);

                    // avoid double-instrumentation
                    if (isHooksCallback(inst)) return false;

                    if (enableWriteTracking) collectStore(inst, stores);
                    if (enableReadTracking) collectLoad(inst, loads);
                    // TODO collect library function calls
                }
            }

            // swizzling buffers
            LLVMContext &context = F.getContext();
            IntegerType *intType = IntegerType::get(context, 64);
            Instruction *entryPoint = F.getEntryBlock().getFirstNonPHI();
            AllocaInst *storeSwizzlingBuffer = new AllocaInst(intType, 0,
                    "storeSwizzlingBuffer", entryPoint);
            AllocaInst *loadSwizzlingBuffer = new AllocaInst(intType, 0,
                    "loadSwizzlingBuffer", entryPoint);

            // post-callback buffers
            AllocaInst *postStoreBuffers[2];
            if (enablePostWriteCallback) {
                postStoreBuffers[0] = new AllocaInst(intType, 0,
                        "postStoreAddrBuffer", entryPoint);
                postStoreBuffers[1] = new AllocaInst(intType, 0,
                        "postStoreSizeBuffer", entryPoint);
            }
            AllocaInst *postLoadBuffers[2];
            if (enablePostReadCallback) {
                postLoadBuffers[0] = new AllocaInst(intType, 0,
                        "postLoadAddrBuffer", entryPoint);
                postLoadBuffers[1] = new AllocaInst(intType, 0,
                        "postLoadSizeBuffer", entryPoint);
            }



	    std::vector<Store *> RAWstores;
            collectStoreFromAntidependencyPairs(RAWstores);

	    int stack_store_size = 0;
            // process the list of loads and stores
            for (auto it = stores.begin(); it != stores.end(); it++) {
		Instruction *inst = getInstFromAccess(*it);
		bool RAWstore = false;
		for(auto RAWit = RAWstores.begin(); RAWit!=RAWstores.end(); RAWit++){
		    if(getInstFromAccess(*RAWit) == getInstFromAccess(*it)){
			RAWstore = true;
			break;
		    }
		}
		if(RAWstore){
		    retVal |= runOnRAWAccess(*it, storeSwizzlingBuffer, postStoreBuffers);
                    delete *it;
		}
		else{
		    if (isOnStack(getInstFromAccess(*it), getPtrFromAccess(*it)))
			stack_store_size++;
                    retVal |= runOnAccess(*it, storeSwizzlingBuffer, postStoreBuffers);
                    delete *it;
		}
            }
            stores.clear();
            for (auto it = loads.begin(); it != loads.end(); it++) {
                retVal |= runOnAccess(*it, loadSwizzlingBuffer, postLoadBuffers);
                delete *it;
            }
            loads.clear();

            // TODO tracking and swizzling for library calls

            return retVal;
        }


	virtual void getAnalysisUsage(AnalysisUsage &AU) const {
          AU.addRequired<MemoryIdempotenceAnalysis>();
          AU.setPreservesAll();
        }


    protected:
        std::string rangeCheckFunction = "is_nvmm";
	std::string RAWwriteTrackFunction = "on_RAW_write";
        std::string writeTrackFunction = "on_nvmm_write";
        std::string readTrackFunction = "on_nvmm_read";
        std::string swizzlingFunction = "to_absolute_ptr";
        std::string postStoreFunction = "post_nvmm_write";
        std::string postLoadFunction = "post_nvmm_read";
        std::string nvmmStrLen = "nvmm_strlen";
        std::string nvmmStrCmp = "nvmm_strcmp";
        std::string nvmmMemCmp = "nvmm_memcmp";

        bool enableWriteTracking = true;
        bool enableReadTracking = true;
        bool enableRangeCheck = true;
        bool enableStaticRangeCheck = false;
        bool enableSwizzling = true;
        bool enablePostWriteCallback = false;
        bool enablePostReadCallback = false;
    };
}

// LLVM uses ID’s address to identify a pass, so initialization value is not important.
char NaiveHook::ID = 0;

static RegisterPass<NaiveHook> h1("naivehook", "Hook pass that instrument callbacks");

static void registerNaiveHook(const PassManagerBuilder &,
        legacy::PassManagerBase &PM) {
    PM.add(new NaiveHook());
}

static RegisterStandardPasses RegisterNaiveHook(
        PassManagerBuilder::EP_EarlyAsPossible, registerNaiveHook);
