#include "Common.hpp"

//===----------------------------------------------------------------------===//
//
// This file contains the implementation for instrument (volatile) global variables
// recording callbacks at the LLVM IR level.
// The implementation is based on the code provided in (unpublished)
// "NVHooks: A Flexible, Optimizing Compiler for Non-Volatile Memory Programming".
// See "Clobber-NVM: Log Less, Re-execute More" in ASPLOS'21
//
//
//===----------------------------------------------------------------------===//


namespace {
    class MemoryAccess {
    public:
        Instruction *inst;
        Value *ptr;
        Type *type;
        Value *size;
        std::vector<Instruction *> nvmBlock;
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

    class GlobalVal : public FunctionPass {
    public:
        static char ID;
        GlobalVal() : FunctionPass(ID) { }

        bool doInitialization(Module &M) {
            return true;
        }

        /*
         * helper (utility) methods
         */

        virtual bool isGlobal(Instruction *inst, Value *value) {
	     if(isa<GlobalVariable>(value))
		return true;
	    return false;
	}


        virtual bool isOnStack(Instruction *inst, Value *value) {
            if (isa<ConstantExpr>(value)){
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
		 return true;
	    }
            return false;
        }


        template <class T>
        Instruction *createOnsiteRangeTrack(Instruction *inst,
                Value *addr, Value *size) {
            std::string trackerFunction = onsiteGlobalTrackFunction;

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
	bool runOnLock(T *access, std::vector<Load *> &loads){
	    bool retVal = true;
            for (auto it = loads.begin(); it != loads.end(); it++) {
                retVal |= runOnLockAccess(access,*it);
                delete *it;
            }
            loads.clear();	
	    return retVal;
	}

        template <class T>
        bool runOnLockAccess(Store *lock,T *access) {
            if (!isGlobal(access->inst, access->ptr))
                return false;

            LLVMContext &context = access->inst->getContext();
            PointerType *ptrType = Type::getIntNPtrTy(context, 8); // 64?

            Instruction *inst = lock->inst;
            Value *addr = access->ptr;
            Value *size = access->size;

            if (addr->getType() != ptrType) {
                addr = new BitCastInst(addr, ptrType, "", inst);
            }

            Instruction *accessTrack = NULL;
            accessTrack = createOnsiteRangeTrack<T>(inst,
                    access->elementPtr == NULL ? addr : access->elementPtr, size);

            return true;
        }



        template <class T>
        bool runOnAccess(T *access) {
            if (!isGlobal(access->inst, access->ptr))
		return false;
            LLVMContext &context = access->inst->getContext();
            PointerType *ptrType = Type::getIntNPtrTy(context, 8); 

            Instruction *inst = access->inst;
            Value *addr = access->ptr;
            Value *size = access->size;

            if (addr->getType() != ptrType) {
                addr = new BitCastInst(addr, ptrType, "", inst);
            }

	    Instruction *accessTrack = NULL;
	    accessTrack = createOnsiteRangeTrack<T>(inst,
                    access->elementPtr == NULL ? addr : access->elementPtr, size);

            return true;
        }


	bool isLock(Instruction *inst, std::vector<Store *> &lock){
            if (isa<CallInst>(inst)){

                CallInst *call = cast<CallInst>(inst);
               
                Function *F = call->getCalledFunction();
                if((F->getName().str() == "nvm_my_lock")||(F->getName().str() == "pthread_mutex_lock")||(F->getName().str() == "pthread_rwlock_wrlock")){
                    LLVMContext &context = inst->getModule()->getContext();
                
                    if(isa<PointerType>(call->getArgOperand(0)->getType())){
                        Value *size = ConstantInt::get(IntegerType::get(context, 64), sizeof(uint64_t), false);
                        LLVMContext &context = inst->getContext();
                        PointerType *ptrType = Type::getIntNPtrTy(context, 8);
                        Store *s = new Store(inst, call->getArgOperand(0), ptrType->getElementType(), size);
                        lock.push_back(s);
			return true;
                     }
		}
	    }

	    return false;
	}

        /*
         * collection and initialization methods
         */

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
            } 
        }


        bool runOnFunction(Function &F) override {

            bool retVal = false;
            std::vector<Store *> stores;
            std::vector<Load *> loadsbefore;
	    std::vector<Load *> loadsafter;

	    bool locked = false;
            for (BBIterator bi = F.getBasicBlockList().begin();
                    bi != F.getBasicBlockList().end(); bi++) {
                BasicBlock *bb = &(*bi);
                for (InstIterator ii = bb->begin(); ii != bb->end(); ii++) {
                    Instruction *inst = &(*ii);
		    if(isLock(inst, stores)) locked = true;
                    if(!locked) collectLoad(inst, loadsbefore);
		    if(locked) collectLoad(inst, loadsafter);
                }
            }

	    for (auto it = stores.begin(); it != stores.end(); it++) {
		retVal |= runOnLock(*it, loadsafter);
		delete *it;
	    }
	    stores.clear();


            for (auto it = loadsbefore.begin(); it != loadsbefore.end(); it++) {
                retVal |= runOnAccess(*it);
                delete *it;
            }
            loadsbefore.clear();

            return retVal;
        }


	virtual void getAnalysisUsage(AnalysisUsage &AU) const {
          AU.setPreservesAll();
        }


    protected:
	std::string onsiteGlobalTrackFunction = "onsite_global_track";
    };
}

// LLVM uses ID’s address to identify a pass, so initialization value is not important.
char GlobalVal::ID = 0;

static RegisterPass<GlobalVal> h1("globalval", "deal with global variable for vlog");

static void registerGlobalVal(const PassManagerBuilder &,
        legacy::PassManagerBase &PM) {
    PM.add(new GlobalVal());
}

static RegisterStandardPasses RegisterGlobalVal(
        PassManagerBuilder::EP_EarlyAsPossible, registerGlobalVal);
