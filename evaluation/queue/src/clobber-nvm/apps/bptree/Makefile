CXX:=g++
CLANG=$(realpath ../../build/bin/clang)
OPT=$(realpath ../../build/bin/opt)
ROLLCLANG:=$(realpath ../../rollclang)
CLOBBERLOGCLANG:=$(realpath ../../clobberlogclang)
VLOGCLANG_HRS:=$(realpath ../../vlogclang_hrs)
CLOBBERCLANG:=$(realpath ../../clobberclang)
CFLAGS:= -Wall -o3
ATLAS_PLUGIN:=$(realpath ../../Atlas/compiler-plugin/plugin_build/NvmInstrumenter.so)
NVFLAGS:='-pointer-swizzling -load-tracking -static-range-check -coalesce-tracking-callbacks -coalesce-tracking-loops -unused-tracking'
V_SRC:=benchmark.c tree.c
CLANG_3_9:=/usr/lib/llvm-3.9/bin/clang
ATLAS:=$(CLANG_3_9) -Xclang -load -Xclang $(ATLAS_PLUGIN)
JEMALLOC:=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir`i -ljemalloc `jemalloc-config --libs`

TARGET:=benchmark

all: $(TARGET)

wrap/context.o: ../context/context.c ../context/context.h
	$(CLANG) $(CFLAGS) -O3 -c -o $@ ../context/context.c

wrap/clobber.o: wrap/clobber.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/undo.o: wrap/undo.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/vlog.o: wrap/vlog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/nolog.o: wrap/nolog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/warlog.o: wrap/warlog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/count.o: wrap/count.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/admin_pop.o: wrap/admin_pop.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^ 

wrap/context-custom.o: ../context/context.c ../context/context.h
	$(CLANG) $(CFLAGS) -O3 -DKAMINO -DCUSTOM_PTHREAD_CREATE -c $< -o $@

wrap/context-atlas.o: ../context/atlas.c
	$(CLANG) $(CFLAGS) -O3 -c $^ -o $@

wrap/context_noadminpop.o: ../context/context_noadminpop.c
	$(CLANG) $(CFLAGS) -O3 -c $^ -o $@

wrap/atlas.o: wrap/atlas.c
	$(CLANG) $(CFLAGS) -O3 -c $^ -o $@

wrap/atlas-config.h: ../../Atlas/runtime/src/internal_includes/pregion_configs.hpp
	cp $^ $@
	sed -i -e '20d;42d;' $@

wrap/atlas-hooks.o: wrap/atlas-hooks.c wrap/atlas-config.h
	$(CLANG) $(CFLAGS) -O3 -c $< -o $@

$(TARGET)-atlas: $(V_SRC) wrap/atlas-hooks.o wrap/context_noadminpop.o
	DUMP_LLVM_IR=1 NVHFLAGS="$(NVHFLAGS) -post-store-callback" $(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree.c -o tree.o
	$(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree_update.c -o treeupdate.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT benchmark.o tree.o treeupdate.o \
		wrap/context_noadminpop.o wrap/atlas-hooks.o \
		-Wl,--wrap=pthread_join \
		-Wl,--wrap=pthread_create\
		-Wl,--wrap=pthread_rwlock_rdlock\
		-Wl,--wrap=pthread_rwlock_wrlock\
		-Wl,--wrap=pthread_rwlock_unlock\
		-Wl,--wrap=pthread_rwlock_init\
		-Wl,--wrap=pthread_mutex_init\
		-Wl,--wrap=pthread_mutex_lock\
		-Wl,--wrap=pthread_mutex_unlock\
		-Wl,--wrap=malloc \
		-Wl,--wrap=free \
		-Wl,--wrap=calloc \
		-Wl,--wrap=memalign\
		-lpthread -L ../../Atlas/runtime/build-all/lib -latlas-shared $(JEMALLOC)


$(TARGET)-warlog: $(V_SRC) wrap/warlog.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree.c -o tree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLOBBERCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree_update.c -o treeupdate.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                tree.o benchmark.o wrap/warlog.o wrap/context.o wrap/admin_pop.o treeupdate.o\
                -lpthread -lpmemobj $(JEMALLOC)




$(TARGET)-clobber: $(V_SRC) wrap/clobber.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree.c -o tree.o
	$(VLOGCLANG_HRS) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLOBBERCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree_update.c -o treeupdate.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                tree.o benchmark.o wrap/clobber.o wrap/context.o wrap/admin_pop.o treeupdate.o\
                -lpthread -lpmemobj $(JEMALLOC)

$(TARGET)-undo: $(V_SRC) wrap/undo.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree.c -o tree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree_update.c -o treeupdate.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                wrap/undo.o wrap/context.o wrap/admin_pop.o tree.o benchmark.o treeupdate.o\
                -lpthread -lpmemobj $(JEMALLOC)

$(TARGET)-vlog: $(V_SRC) wrap/vlog.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree.c -o tree.o
	$(VLOGCLANG_HRS) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree_update.c -o treeupdate.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                tree.o benchmark.o wrap/vlog.o wrap/context.o wrap/admin_pop.o treeupdate.o\
                -lpthread -lpmemobj $(JEMALLOC)

$(TARGET)-nolog: $(V_SRC) wrap/nolog.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree.c -o tree.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree_update.c -o treeupdate.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                tree.o benchmark.o wrap/nolog.o wrap/context.o wrap/admin_pop.o treeupdate.o\
                -lpthread -lpmemobj $(JEMALLOC)



$(TARGET)-count: $(V_SRC) wrap/count.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree.c -o tree.o
	$(VLOGCLANG_HRS) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLOBBERCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c tree_update.c -o treeupdate.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                tree.o benchmark.o wrap/count.o wrap/context.o wrap/admin_pop.o treeupdate.o\
                -lpthread -lpmemobj $(JEMALLOC)



clean:
	$(RM) -f $(TARGET)
	$(RM) -f benchmark-*
	$(RM) -f wrap/*.o
	$(RM) -f *.o*
	$(RM) -f *.ll.* *.o.ll *.o.bc
	$(RM) -f *.o *.ll

