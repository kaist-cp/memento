CXX:=g++
CLANG:=$(realpath ../../build/bin/clang)
ROLLCLANG:=$(realpath ../../rollclang)
CLOBBERLOGCLANG:=$(realpath ../../clobberlogclang)
VLOGCLANG_HRS:=$(realpath ../../vlogclang_hrs)
CLOBBERCLANG:=$(realpath ../../clobberclang)

CFLAGS:=-Wall -o3
NVFLAGS:='-pointer-swizzling -load-tracking -static-range-check -coalesce-tracking-callbacks -coalesce-tracking-loops -unused-tracking'
V_SRC:=benchmark.c benchmark-v.c benchmark.h hashmap_v.h hashmap_v.c
ATLAS_PLUGIN:=$(realpath ../../libs/Atlas/compiler-plugin/plugin_build/NvmInstrumenter.so)
CLANG_3_9:=/usr/lib/llvm-3.9/bin/clang
ATLAS:=$(CLANG_3_9) -Xclang -load -Xclang $(ATLAS_PLUGIN)
JEMALLOC:=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir`i -ljemalloc `jemalloc-config --libs`

TARGET:=benchmark

all: $(TARGET) $(TARGET)-v $(TARGET)-pmdk $(TARGET)-pmdk-opt

$(TARGET): benchmark.c benchmark-tx.c benchmark.h
	gcc $(CFLAGS) -o $@ benchmark.c benchmark-tx.c \
		-I../../pmdk-1.6/src/examples/libpmemobj/hashmap \
		-L../../pmdk-1.6/src/examples/libpmemobj/hashmap -lhashmap_tx \
		-lpmemobj -lpthread $(JEMALLOC)

$(TARGET)-v: benchmark.c benchmark-v.c benchmark.h hashmap_v.h hashmap_v.c
	$(CLANG) $(CFLAGS) -o $@ benchmark.c benchmark-v.c hashmap_v.c -lpthread $(JEMALLOC)

wrap/clobber.o: wrap/clobber.c
	$(CLANG) $(CFLAGS) -O3 -c $^ -o $@

wrap/undo.o: wrap/undo.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/vlog.o: wrap/vlog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/nolog.o: wrap/nolog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/count.o: wrap/count.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 


wrap/admin_pop.o: wrap/admin_pop.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/context.o: ../context/context.c ../context/context.h
	$(CLANG) $(CFLAGS) -O3 -c $< -o $@

wrap/context-custom.o: ../context/context.c ../context/context.h
	$(CLANG) $(CFLAGS) -O3 -DCUSTOM_PTHREAD_CREATE -c $< -o $@



$(TARGET)-clobber: $(V_SRC) wrap/clobber.o wrap/context.o wrap/admin_pop.o
	$(VLOGCLANG_HRS) $(CFLAGS) -c benchmark.c -o benchmark.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -c benchmark-v.c -o benchmark-v.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -c hashmap_v.c -o hashmap_v.o
	$(CLANG) -o $@ benchmark.o benchmark-v.o hashmap_v.o wrap/clobber.o wrap/context.o wrap/admin_pop.o\
                -Wl,--wrap=pthread_mutex_lock \
                -Wl,--wrap=pthread_mutex_unlock \
                -Wl,--wrap=pthread_rwlock_rdlock \
                -Wl,--wrap=pthread_rwlock_wrlock \
                -Wl,--wrap=pthread_rwlock_unlock \
                -Wl,--wrap=pthread_rwlock_init \
                -Wl,--wrap=pthread_join \
                -Wl,--wrap=pthread_create \
                -Wl,--wrap=malloc \
                -Wl,--wrap=free \
                -Wl,--wrap=memalign \
                -Wl,--wrap=hashmap_recover \
                -lpthread -lpmemobj $(JEMALLOC)




wrap/warlog.o: wrap/warlog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 


$(TARGET)-warlog: $(V_SRC) wrap/warlog.o wrap/context.o wrap/admin_pop.o
	$(CLANG) $(CFLAGS) -c benchmark.c -o benchmark.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -c benchmark-v.c -o benchmark-v.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -c hashmap_v.c -o hashmap_v.o
	$(CLANG) -o $@ benchmark.o benchmark-v.o hashmap_v.o wrap/warlog.o wrap/context.o wrap/admin_pop.o\
                -Wl,--wrap=pthread_mutex_lock \
                -Wl,--wrap=pthread_mutex_unlock \
                -Wl,--wrap=pthread_rwlock_rdlock \
                -Wl,--wrap=pthread_rwlock_wrlock \
                -Wl,--wrap=pthread_rwlock_unlock \
                -Wl,--wrap=pthread_rwlock_init \
                -Wl,--wrap=pthread_join \
                -Wl,--wrap=pthread_create \
                -Wl,--wrap=malloc \
                -Wl,--wrap=free \
                -Wl,--wrap=memalign \
                -Wl,--wrap=hashmap_recover \
                -lpthread -lpmemobj $(JEMALLOC)




$(TARGET)-undo: $(V_SRC) wrap/undo.o wrap/context.o wrap/admin_pop.o
	$(CLANG) $(CFLAGS) -c benchmark.c -o benchmark.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -c benchmark-v.c -o benchmark-v.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -c hashmap_v.c -o hashmap_v.o
	$(CLANG) -o $@ benchmark.o benchmark-v.o hashmap_v.o wrap/undo.o wrap/context.o wrap/admin_pop.o\
                -Wl,--wrap=pthread_mutex_lock \
                -Wl,--wrap=pthread_mutex_unlock \
                -Wl,--wrap=pthread_rwlock_rdlock \
                -Wl,--wrap=pthread_rwlock_wrlock \
                -Wl,--wrap=pthread_rwlock_unlock \
                -Wl,--wrap=pthread_rwlock_init \
                -Wl,--wrap=pthread_join \
                -Wl,--wrap=pthread_create \
                -Wl,--wrap=malloc \
                -Wl,--wrap=free \
                -Wl,--wrap=memalign \
                -Wl,--wrap=hashmap_recover \
                -lpthread -lpmemobj $(JEMALLOC)

$(TARGET)-vlog: $(V_SRC) wrap/vlog.o wrap/context.o wrap/admin_pop.o
	$(VLOGCLANG_HRS) $(CFLAGS) -c benchmark.c -o benchmark.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -c benchmark-v.c -o benchmark-v.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -c hashmap_v.c -o hashmap_v.o
	$(CLANG) -o $@ benchmark.o benchmark-v.o hashmap_v.o wrap/vlog.o wrap/context.o wrap/admin_pop.o\
                -Wl,--wrap=pthread_mutex_lock \
                -Wl,--wrap=pthread_mutex_unlock \
                -Wl,--wrap=pthread_rwlock_rdlock \
                -Wl,--wrap=pthread_rwlock_wrlock \
                -Wl,--wrap=pthread_rwlock_unlock \
                -Wl,--wrap=pthread_rwlock_init \
                -Wl,--wrap=pthread_join \
                -Wl,--wrap=pthread_create \
                -Wl,--wrap=malloc \
                -Wl,--wrap=free \
                -Wl,--wrap=memalign \
                -Wl,--wrap=hashmap_recover \
                -lpthread -lpmemobj $(JEMALLOC)

$(TARGET)-nolog: $(V_SRC) wrap/nolog.o wrap/context.o wrap/admin_pop.o
	$(CLANG) $(CFLAGS) -c benchmark.c -o benchmark.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -c benchmark-v.c -o benchmark-v.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -c hashmap_v.c -o hashmap_v.o
	$(CLANG) -o $@ benchmark.o benchmark-v.o hashmap_v.o wrap/nolog.o wrap/context.o wrap/admin_pop.o\
                -Wl,--wrap=pthread_mutex_lock \
                -Wl,--wrap=pthread_mutex_unlock \
                -Wl,--wrap=pthread_rwlock_rdlock \
                -Wl,--wrap=pthread_rwlock_wrlock \
                -Wl,--wrap=pthread_rwlock_unlock \
                -Wl,--wrap=pthread_rwlock_init \
                -Wl,--wrap=pthread_join \
                -Wl,--wrap=pthread_create \
                -Wl,--wrap=malloc \
                -Wl,--wrap=free \
                -Wl,--wrap=memalign \
                -Wl,--wrap=hashmap_recover \
                -lpthread -lpmemobj $(JEMALLOC)

$(TARGET)-count: $(V_SRC) wrap/count.o wrap/context.o wrap/admin_pop.o
	$(VLOGCLANG_HRS) $(CFLAGS) -c benchmark.c -o benchmark.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -c benchmark-v.c -o benchmark-v.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -c hashmap_v.c -o hashmap_v.o
	$(CLANG) -o $@ benchmark.o benchmark-v.o hashmap_v.o wrap/count.o wrap/context.o wrap/admin_pop.o\
                -Wl,--wrap=pthread_mutex_lock \
                -Wl,--wrap=pthread_mutex_unlock \
                -Wl,--wrap=pthread_rwlock_rdlock \
                -Wl,--wrap=pthread_rwlock_wrlock \
                -Wl,--wrap=pthread_rwlock_unlock \
                -Wl,--wrap=pthread_rwlock_init \
                -Wl,--wrap=pthread_join \
                -Wl,--wrap=pthread_create \
                -Wl,--wrap=malloc \
                -Wl,--wrap=free \
                -Wl,--wrap=memalign \
                -Wl,--wrap=hashmap_recover \
                -lpthread -lpmemobj $(JEMALLOC)


wrap/atlas-config.h: ../../Atlas/runtime/src/internal_includes/pregion_configs.hpp
	cp $^ $@
	sed -i -e '20d;42d;' $@

wrap/atlas-hooks.o: wrap/atlas-hooks.c
	$(CLANG) $(CFLAGS) -O3 -c $^ -o $@

wrap/atlas-hooks-base.o: ../bptree/wrap/atlas-hooks.c wrap/atlas-config.h
	$(CLANG) $(CFLAGS) -O3 -DHOOKS_HASHMAP -c $< -o $@

wrap/context_noadminpop.o: ../context/context_noadminpop.c
	$(CLANG) $(CFLAGS) -O3 -c $^ -o $@

$(TARGET)-atlas: $(V_SRC) wrap/atlas-hooks.o wrap/context_noadminpop.o wrap/atlas-hooks-base.o
	$(CLANG) $(CFLAGS) -c benchmark.c -o benchmark.o
	DUMP_LLVM_IR=1 NVHFLAGS="$(NVHFLAGS) -post-store-callback" $(ROLLCLANG) $(CFLAGS) -c benchmark-v.c -o benchmark-c.o
	DUMP_LLVM_IR=1 NVHFLAGS="$(NVHFLAGS) -post-store-callback" $(ROLLCLANG) $(CFLAGS) -c hashmap_v.c -o hashmap_v.o
	$(CLANG) -o $@ benchmark.o benchmark-c.o hashmap_v.o \
		wrap/atlas-hooks.o wrap/atlas-hooks-base.o wrap/context_noadminpop.o \
		-Wl,--wrap=pthread_mutex_lock \
		-Wl,--wrap=pthread_mutex_unlock \
		-Wl,--wrap=pthread_rwlock_rdlock \
		-Wl,--wrap=pthread_rwlock_wrlock \
		-Wl,--wrap=pthread_rwlock_unlock \
		-Wl,--wrap=pthread_rwlock_init \
		-Wl,--wrap=pthread_join \
		-Wl,--wrap=malloc \
		-Wl,--wrap=free \
		-Wl,--wrap=memalign \
		-Wl,--wrap=pthread_create \
		-Wl,--wrap=hashmap_recover \
		-Wl,--wrap=hashmap_close \
		-lpthread -L ../../Atlas/runtime/build-all/lib -latlas-shared $(JEMALLOC)


clean: 
	$(RM) -f $(TARGET)
	$(RM) -f $(TARGET)-clobber
	$(RM) -f $(TARGET)-undo
	$(RM) -f $(TARGET)-warlog
	$(RM) -f $(TARGET)-nolog
	$(RM) -f $(TARGET)-count
	$(RM) -f $(TARGET)-vlog
	$(RM) -f $(TARGET)-atlas
	$(RM) -f *.o* wrap/*.o
	$(RM) -f *.bc
	$(RM) -f *.ll
	$(RM) -f *.ll.2
