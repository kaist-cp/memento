CXX:=g++
CLANG=$(realpath ../../../build/bin/clang)
OPT=$(realpath ../../../build/bin/opt)
ROLLINLINECLANG:=$(realpath ../../../clobberlogclang)
CFLAGS:= -Wall -o3
CFLAGS   += -I$(LIB)
CFLAGS += -DLIST_NO_DUPLICATES
CFLAGS += -DMAP_USE_AVLTREE_LONG -DUSE_DUP_AND_REL
NVFLAGS:='-pointer-swizzling -load-tracking -static-range-check -coalesce-tracking-callbacks -coalesce-tracking-loops -unused-tracking'
LIB := ../lib
V_LIB:=$(LIB)/list.c $(LIB)/avltree_long.c $(LIB)/hash.c $(LIB)/hashtable.c $(LIB)/pair.c $(LIB)/mt19937ar.c $(LIB)/random.c $(LIB)/rbtree.c $(LIB)/thread.c
CLANG_3_9:=/usr/lib/llvm-3.9/bin/clang
JEMALLOC:=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir`i -ljemalloc `jemalloc-config --libs`

TARGET:=vacation


all: $(TARGET)

../lib/wrap/clobber_rbtree.o: ../lib/wrap/clobber_rbtree.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

../lib/wrap/admin_pop.o: ../lib/wrap/admin_pop.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

../lib/wrap/context.o: ../lib/wrap/context.c ../lib/wrap/context.h
	$(CLANG) $(CFLAGS) -O3 -c -o $@ ../lib/wrap/context.c


$(TARGET)-clobber-rbtree: $(V_LIB) client.c customer.c manager.c manager.h reservation.c vacation.c
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c client.c -o client.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c customer.c -o customer.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c manager.c -o manager.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c reservation.c -o reservation.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vacation.c -o vacation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/list.c -o list.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/pair.c -o pair.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/mt19937ar.c -o mt19937ar.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/rbtree.c -o rbtree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/thread.c -o thread.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
				-Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                vacation.o client.o customer.o manager.o reservation.o list.o pair.o\
                mt19937ar.o random.o rbtree.o thread.o\
                 ../lib/wrap/clobber_rbtree.o ../lib/wrap/context.o ../lib/wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)



../lib/wrap/pmdk_rbtree.o: ../lib/wrap/pmdk_rbtree.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

$(TARGET)-pmdk-rbtree: $(V_LIB) client.c customer.c manager.c manager.h reservation.c vacation.c
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c client.c -o client.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c customer.c -o customer.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c manager.c -o manager.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c reservation.c -o reservation.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vacation.c -o vacation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/list.c -o list.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/pair.c -o pair.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/mt19937ar.c -o mt19937ar.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/rbtree.c -o rbtree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/thread.c -o thread.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                vacation.o client.o customer.o manager.o reservation.o list.o pair.o\
                mt19937ar.o random.o rbtree.o thread.o\
				../lib/wrap/pmdk_rbtree.o ../lib/wrap/context.o ../lib/wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)



../lib/wrap/nolog_rbtree.o: ../lib/wrap/nolog_rbtree.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

$(TARGET)-nolog-rbtree: $(V_LIB) client.c customer.c manager.c manager.h reservation.c vacation.c
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c client.c -o client.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c customer.c -o customer.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c manager.c -o manager.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c reservation.c -o reservation.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vacation.c -o vacation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/list.c -o list.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/pair.c -o pair.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/mt19937ar.c -o mt19937ar.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/rbtree.c -o rbtree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/thread.c -o thread.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                vacation.o client.o customer.o manager.o reservation.o list.o pair.o\
                mt19937ar.o random.o rbtree.o thread.o\
                 ../lib/wrap/nolog_rbtree.o ../lib/wrap/context.o ../lib/wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)


../lib/wrap/clobber_avltree.o: ../lib/wrap/clobber_avltree.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

$(TARGET)-clobber-avltree: $(V_LIB) client.c customer.c manager_avltree.c manager.h reservation.c vacation.c
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c client.c -o client.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c customer.c -o customer.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c manager_avltree.c -o manager.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c reservation.c -o reservation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vacation.c -o vacation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/list.c -o list.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/pair.c -o pair.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/mt19937ar.c -o mt19937ar.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/hash.c -o hash.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/avltree_long.c -o avltree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/thread.c -o thread.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                vacation.o client.o customer.o manager.o reservation.o list.o pair.o\
                mt19937ar.o random.o avltree.o hash.o thread.o\
                 ../lib/wrap/clobber_avltree.o ../lib/wrap/context.o ../lib/wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)


../lib/wrap/pmdk_avltree.o: ../lib/wrap/pmdk_avltree.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^

$(TARGET)-pmdk-avltree: $(V_LIB) client.c customer.c manager_avltree.c manager.h reservation.c vacation.c
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c client.c -o client.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c customer.c -o customer.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c manager_avltree.c -o manager.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c reservation.c -o reservation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vacation.c -o vacation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/list.c -o list.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/pair.c -o pair.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/mt19937ar.c -o mt19937ar.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/hash.c -o hash.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/avltree_long.c -o avltree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/thread.c -o thread.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                vacation.o client.o customer.o manager.o reservation.o list.o pair.o\
                mt19937ar.o random.o avltree.o hash.o thread.o\
                 ../lib/wrap/pmdk_avltree.o ../lib/wrap/context.o ../lib/wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)


../lib/wrap/nolog_avltree.o: ../lib/wrap/nolog_avltree.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^

$(TARGET)-nolog-avltree: $(V_LIB) client.c customer.c manager_avltree.c manager.h reservation.c vacation.c
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c client.c -o client.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c customer.c -o customer.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c manager_avltree.c -o manager.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c reservation.c -o reservation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vacation.c -o vacation.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/list.c -o list.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/pair.c -o pair.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/mt19937ar.c -o mt19937ar.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/hash.c -o hash.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/avltree_long.c -o avltree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c $(LIB)/thread.c -o thread.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                vacation.o client.o customer.o manager.o reservation.o list.o pair.o\
                mt19937ar.o random.o avltree.o hash.o thread.o\
                 ../lib/wrap/nolog_avltree.o ../lib/wrap/context.o ../lib/wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)


clean:
	$(RM) -f $(TARGET)-*
	$(RM) -f ../lib/wrap/*.o
	$(RM) -f build.log
	$(RM) -f *.o*
	$(RM) -f *.ll.* *.o.ll *.o.bc
