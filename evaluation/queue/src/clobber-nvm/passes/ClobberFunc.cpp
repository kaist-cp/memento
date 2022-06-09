#include "Common.hpp"
#include "MemoryIdempotenceAnalysis.h"



//===----------------------------------------------------------------------===//
//
// This file contains the implementation for instrument clobber function info recording
// callbacks at the LLVM IR level.
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

    class ClobberFunc : public FunctionPass {
    public:
        static char ID;
        ClobberFunc() : FunctionPass(ID) { }

        bool doInitialization(Module &M) {
            return true;
        }

        /*
         * helper (utility) methods
         */


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
	Instruction *createParaRecord(Instruction *inst,
                Value *addr, Value *size) {
            std::string trackerFunction = paraRecordFunction;

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
        Instruction *createNvmPtrParaRecord(Instruction *inst,
                Value *addr, Value *size) {
            std::string trackerFunction = NvmPtrparaRecordFunction;

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



        Instruction *createFuncIndex(Instruction *inst, Value *index) {
            std::string indexFunction = FuncIndexFunction;

            Module *m = static_cast<Module *>(inst->getModule());
            FunctionType *funcType =
                TypeBuilder<void(uint8_t), false>::get(m->getContext());
            Function *func = (Function*)m->getOrInsertFunction(indexFunction, funcType);

            std::vector<Value *> varg_list;
            varg_list.push_back(index);

            CallInst *indexRecordingCall = CallInst::Create(func, varg_list, "", inst);
            indexRecordingCall->setDebugLoc(inst->getDebugLoc());
            return indexRecordingCall;
        }




        template <class T>
        bool runOnCalledFuncwithPtr(T* access){

            LLVMContext &context = access->inst->getContext();
            PointerType *ptrType = Type::getIntNPtrTy(context, 8); // 64?

            Instruction *inst = access->inst;
            Value *addr = access->ptr;
            Value *size = access->size;

            if (addr->getType() != ptrType) {
                addr = new BitCastInst(addr, ptrType, "", inst);
            }


            Instruction *accessTrack = NULL;
            accessTrack = createNvmPtrParaRecord<T>(inst,
                    access->elementPtr == NULL ? addr : access->elementPtr, size);

            return true;
        }




	template <class T>
	bool runOnCalledFunc(T* access){

            LLVMContext &context = access->inst->getContext();
            PointerType *ptrType = Type::getIntNPtrTy(context, 8); // 64?

            Instruction *inst = access->inst;
            Value *addr = access->ptr;
            Value *size = access->size;

            if (addr->getType() != ptrType) {
                addr = new BitCastInst(addr, ptrType, "", inst);
            }


            Instruction *accessTrack = NULL;
            accessTrack = createParaRecord<T>(inst,
                    access->elementPtr == NULL ? addr : access->elementPtr, size);

	    return true;
	}



	void collectPtrPara(Instruction *inst, std::vector<Store *> &stores){
	    if (isa<CallInst>(inst)){

	        CallInst *call = cast<CallInst>(inst);
	        int opnum = call->getNumArgOperands();
	        int i=0;
		Function *F = call->getCalledFunction();

                if((F->getName().str() == "TreeInsert")||(F->getName().str() == "UpdateLeaf")||(F->getName().str() == "doInsert")){
		LLVMContext &context = inst->getModule()->getContext();
		if(F->getName().str() == "TreeInsert"){
		    Value *constOne = ConstantInt::get(IntegerType::get(context, 8), 2, false);		
		    createFuncIndex(inst, constOne);
		}

                if(F->getName().str() == "UpdateLeaf"){
                    Value *constOne = ConstantInt::get(IntegerType::get(context, 8), 3, false);
                    createFuncIndex(inst, constOne);
                }

                if(F->getName().str() == "doInsert"){
                    Value *constOne = ConstantInt::get(IntegerType::get(context, 8), 4, false);
                    createFuncIndex(inst, constOne);
                }


	        while(i<opnum){
		    if((isa<PointerType>(call->getArgOperand(i)->getType()))&&
			(isa<IntegerType>(call->getArgOperand(i+1)->getType()))){
         	 	Value *szMinusOne = call->getArgOperand(i+1);
       	           	Value *size = createIncrement(call, szMinusOne);
           		LLVMContext &context = inst->getContext();
            	       	PointerType *ptrType = Type::getIntNPtrTy(context, 8);
        		Store *s = new Store(inst, call->getArgOperand(i), ptrType->getElementType(), size);
			stores.push_back(s);
		        i++;
       		     }


	            //TODO: support other types of parameters
        	    i++;

    		}
	    }
	}
	}



	void collectnvmPtrPara(Instruction *inst, std::vector<Store *> &nvmptrstores){
	    if (isa<CallInst>(inst)){

	        CallInst *call = cast<CallInst>(inst);
	        int opnum = call->getNumArgOperands();
	        int i=0;
		Function *F = call->getCalledFunction();
                if((F->getName().str() == "TreeInsert")||(F->getName().str() == "UpdateLeaf")||(F->getName().str() == "doInsert")){
		LLVMContext &context = inst->getModule()->getContext();
		if(F->getName().str() == "TreeInsert"){
		    Value *constOne = ConstantInt::get(IntegerType::get(context, 8), 2, false);		
		    createFuncIndex(inst, constOne);
		}

                if(F->getName().str() == "UpdateLeaf"){
                    Value *constOne = ConstantInt::get(IntegerType::get(context, 8), 3, false);
                    createFuncIndex(inst, constOne);
                }


                if(F->getName().str() == "doInsert"){
                    Value *constOne = ConstantInt::get(IntegerType::get(context, 8), 4, false);
                    createFuncIndex(inst, constOne);
                }


	        while(i<opnum){
                    if((isa<PointerType>(call->getArgOperand(i)->getType()))&&
                        (!isa<IntegerType>(call->getArgOperand(i+1)->getType()))){
                        Value *size = ConstantInt::get(IntegerType::get(context, 64), sizeof(uint64_t), false);
                        LLVMContext &context = inst->getContext();
                        PointerType *ptrType = Type::getIntNPtrTy(context, 8);
                        Store *s = new Store(inst, call->getArgOperand(i), ptrType->getElementType(), size);
                        nvmptrstores.push_back(s);
                     }


	            //TODO: support other types of parameters
        	    i++;

    		}
	    }
	}
	}


        bool runOnFunction(Function &F) override {
	    std::vector<Store *> stores;
	    std::vector<Store *> nvmptrstores;
	    for (BBIterator bi = F.getBasicBlockList().begin();
                    bi != F.getBasicBlockList().end(); bi++) {
                BasicBlock *B = &(*bi);
		for (InstIterator ii = B->begin(); ii != B->end(); ii++){
			Instruction *inst = &(*ii);
			collectPtrPara(inst, stores);
			collectnvmPtrPara(inst, nvmptrstores);

        	}
    	    }

	    bool retVal = false;

            for (auto it = nvmptrstores.begin(); it != nvmptrstores.end(); it++) {
                retVal |= runOnCalledFuncwithPtr(*it);
                delete *it;
            }
            nvmptrstores.clear();

	    for (auto it = stores.begin(); it != stores.end(); it++) {
		retVal |= runOnCalledFunc(*it);
        	delete *it;
	    }
	    stores.clear();
	    return retVal;
	} 

    protected:
	std::string FuncIndexFunction = "add_func_index";
	std::string paraRecordFunction = "ptr_para_record";
	std::string NvmPtrparaRecordFunction = "nvm_ptr_record";
    };
}

// LLVM uses ID’s address to identify a pass, so initialization value is not important.
char ClobberFunc::ID = 0;


static RegisterPass<ClobberFunc> h1("clobberfunc", "Record clobber function related info");

static void registerClobberFunc(const PassManagerBuilder &,
        legacy::PassManagerBase &PM) {
    PM.add(new ClobberFunc());
}

static RegisterStandardPasses RegisterClobberFunc(
        PassManagerBuilder::EP_EarlyAsPossible, registerClobberFunc);
