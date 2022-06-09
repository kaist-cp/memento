CXX:=g++
CLANG=$(realpath ../../../build/bin/clang)
OPT=$(realpath ../../../build/bin/opt)
ROLLINLINECLANG:=$(realpath ../../../clobberlogclang)
CFLAGS:= -Wall -o3
CFLAGS   += -I$(LIB)

CFLAGS += -DLIST_NO_DUPLICATES
CFLAGS += -DMAP_USE_AVLTREE
CFLAGS += -DSET_USE_RBTREE

NVFLAGS:='-pointer-swizzling -load-tracking -static-range-check -coalesce-tracking-callbacks -coalesce-tracking-loops -unused-tracking'
LIB := ../lib
V_LIB:=heap.c avltree.c list.c mt19937ar.c pair.c queue.c random.c rbtree.c thread.c vector.c
CLANG_3_9:=/usr/lib/llvm-3.9/bin/clang
JEMALLOC:=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir`i -ljemalloc `jemalloc-config --libs`

TARGET:=yada



$(TARGET)-clobber: $(V_LIB) coordinate.c element.c mesh.c region.c yada.c lock.c
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c lock.c -o lock.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c yada.c -o yada.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c region.c -o region.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c mesh.c -o mesh.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c element.c -o element.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c coordinate.c -o coordinate.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c heap.c -o heap.o	
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c avltree.c -o avltree.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c list.c -o list.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c mt19937ar.c -o mt19937ar.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c pair.c -o pair.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c rbtree.c -o rbtree.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c thread.c -o thread.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vector.c -o vector.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
		-Wl,--wrap=pthread_rwlock_rdlock\
		-Wl,--wrap=pthread_rwlock_wrlock\
		-Wl,--wrap=pthread_rwlock_unlock\
		-Wl,--wrap=pthread_rwlock_init\
		-Wl,--wrap=pthread_mutex_init\
		-Wl,--wrap=pthread_mutex_lock\
		-Wl,--wrap=pthread_mutex_unlock\
		-Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
		yada.o region.o mesh.o element.o coordinate.o heap.o avltree.o list.o mt19937ar.o\
		pair.o queue.o random.o rbtree.o thread.o vector.o\
		../../runtime/clobber.o ../../runtime/wrapper.o\
		-lpthread -lpmemobj $(JEMALLOC)



$(TARGET)-pmdk: $(V_LIB) coordinate.c element.c mesh.c region.c yada.c lock.c
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c lock.c -o lock.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c yada.c -o yada.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c region.c -o region.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c mesh.c -o mesh.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c element.c -o element.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c coordinate.c -o coordinate.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c heap.c -o heap.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c avltree.c -o avltree.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c list.c -o list.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c mt19937ar.c -o mt19937ar.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c pair.c -o pair.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c rbtree.c -o rbtree.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c thread.c -o thread.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vector.c -o vector.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                yada.o region.o mesh.o element.o coordinate.o heap.o avltree.o list.o mt19937ar.o\
                pair.o queue.o random.o rbtree.o thread.o vector.o\
                ../../runtime/pmdk.o ../../runtime/wrapper.o\
                -lpthread -lpmemobj $(JEMALLOC)

$(TARGET)-nolog: $(V_LIB) coordinate.c element.c mesh.c region.c yada.c lock.c
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c lock.c -o lock.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c yada.c -o yada.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c region.c -o region.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c mesh.c -o mesh.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c element.c -o element.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c coordinate.c -o coordinate.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c heap.c -o heap.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c avltree.c -o avltree.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c list.c -o list.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c mt19937ar.c -o mt19937ar.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c pair.c -o pair.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c random.c -o random.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c rbtree.c -o rbtree.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c thread.c -o thread.o
	DUMP_LLVM_IR=1 $(ROLLINLINECLANG) $(CFLAGS) -fPIC -DPERSISTENT -c vector.c -o vector.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                yada.o region.o mesh.o element.o coordinate.o heap.o avltree.o list.o mt19937ar.o\
                pair.o queue.o random.o rbtree.o thread.o vector.o\
                ../../runtime/nolog.o ../../runtime/wrapper.o\
                -lpthread -lpmemobj $(JEMALLOC)

clean:
	$(RM) -f $(TARGET)-*
	$(RM) -f *.o*
	$(RM) -f *.ll.* *.o.ll *.o.bc
	$(RM) -f build.log
