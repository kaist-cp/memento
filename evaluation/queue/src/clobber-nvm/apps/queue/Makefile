CXX:=g++
CLANG=$(realpath ../../build/bin/clang)
OPT=$(realpath ../../build/bin/opt)
ROLLCLANG:=$(realpath ../../rollclang)
CLOBBERLOGCLANG:=$(realpath ../../clobberlogclang)
VLOGCLANG_HRS:=$(realpath ../../vlogclang_hrs)
CFLAGS:=-Wall -o3
#CFLAGS:=-Wall -ggdb -DDEBUG
NVFLAGS:='-pointer-swizzling -load-tracking -static-range-check -coalesce-tracking-callbacks -coalesce-tracking-loops -unused-tracking'
V_SRC:=benchmark.c list.c
CLANG_3_9:=/usr/lib/llvm-3.9/bin/clang
JEMALLOC:=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir`i -ljemalloc `jemalloc-config --libs`

TARGET:=benchmark

all: $(TARGET)

wrap/clobber.o: wrap/clobber.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^

wrap/undo.o: wrap/undo.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^

wrap/vlog.o: wrap/vlog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^

wrap/nolog.o: wrap/nolog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^

wrap/count.o: wrap/count.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^

wrap/context.o: ../context/context.c ../context/context.h
	$(CLANG) $(CFLAGS) -O3 -c -o $@ ../context/context.c

wrap/admin_pop.o: wrap/admin_pop.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^


$(TARGET)-count: queue.c queue.h benchmark.c wrap/count.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	$(VLOGCLANG_HRS) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                queue.o benchmark.o wrap/count.o wrap/context.o wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)



$(TARGET)-clobber: queue.c queue.h benchmark.c wrap/clobber.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	$(VLOGCLANG_HRS) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                queue.o benchmark.o wrap/clobber.o wrap/context.o wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)

$(TARGET)-undo: queue.c queue.h benchmark.c wrap/undo.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                queue.o benchmark.o wrap/undo.o wrap/context.o wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)


$(TARGET)-vlog: queue.c queue.h benchmark.c wrap/vlog.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	$(VLOGCLANG_HRS) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                queue.o benchmark.o wrap/vlog.o wrap/context.o wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)


$(TARGET)-nolog: queue.c queue.h benchmark.c wrap/nolog.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                queue.o benchmark.o wrap/nolog.o wrap/context.o wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)


wrap/atlas-config.h: ../../Atlas/runtime/src/internal_includes/pregion_configs.hpp
	cp $^ $@
	sed -i -e '20d;42d;' $@

wrap/atlas-hooks.o: wrap/atlas-hooks.c wrap/atlas-config.h
	$(CLANG) $(CFLAGS) -O3 -c $< -o $@

wrap/context_noadminpop.o: ../context/context_noadminpop.c
	$(CLANG) $(CFLAGS) -O3 -c $^ -o $@

$(TARGET)-atlas: queue.c benchmark.c wrap/atlas-hooks.o wrap/context_noadminpop.o
	DUMP_LLVM_IR=1 NVHFLAGS="$(NVHFLAGS) -post-store-callback" $(ROLLCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT benchmark.o queue.o \
		wrap/context_noadminpop.o wrap/atlas-hooks.o \
		-Wl,--wrap=pthread_join \
		-Wl,--wrap=pthread_create\
		-Wl,--wrap=pthread_rwlock_rdlock\
		-Wl,--wrap=pthread_rwlock_wrlock\
		-Wl,--wrap=pthread_rwlock_unlock\
		-Wl,--wrap=pthread_rwlock_init\
		-Wl,--wrap=pthread_mutex_lock\
		-Wl,--wrap=pthread_mutex_unlock\
		-Wl,--wrap=malloc \
		-Wl,--wrap=free \
		-Wl,--wrap=calloc \
		-Wl,--wrap=memalign\
		-lpthread -L ../../Atlas/runtime/build-all/lib -latlas-shared $(JEMALLOC)


wrap/warlog.o: wrap/warlog.c
	$(CLANG) $(CFLAGS) -O3 -c -o $@ $^


$(TARGET)-warlog: queue.c queue.h benchmark.c wrap/warlog.o wrap/context.o wrap/admin_pop.o
	DUMP_LLVM_IR=1 $(CLOBBERLOGCLANG) $(CFLAGS) -fPIC -DPERSISTENT -c queue.c -o queue.o
	$(CLANG) $(CFLAGS) -fPIC -DPERSISTENT -c benchmark.c -o benchmark.o
	$(CLANG) $(CFLAGS) -o $@ -DPERSISTENT -Wl,--wrap=pthread_join -Wl,--wrap=pthread_create\
                -Wl,--wrap=pthread_rwlock_rdlock\
                -Wl,--wrap=pthread_rwlock_wrlock\
                -Wl,--wrap=pthread_rwlock_unlock\
                -Wl,--wrap=pthread_rwlock_init\
                -Wl,--wrap=pthread_mutex_init\
                -Wl,--wrap=pthread_mutex_lock\
                -Wl,--wrap=pthread_mutex_unlock\
                -Wl,--wrap=malloc -Wl,--wrap=free -Wl,--wrap=calloc -Wl,--wrap=memalign\
                queue.o benchmark.o wrap/warlog.o wrap/context.o wrap/admin_pop.o\
                -lpthread -lpmemobj $(JEMALLOC)


clean:
	$(RM) -f $(TARGET)
	$(RM) -f $(TARGET)-undo
	$(RM) -f $(TARGET)-atlas
	$(RM) -f $(TARGET)-clobber
	$(RM) -f $(TARGET)-vlog
	$(RM) -f $(TARGET)-warlog
	$(RM) -f $(TARGET)-nolog
	$(RM) -f $(TARGET)-count
	$(RM) -f *.o* wrap/*.o
	$(RM) -f *.bc
	$(RM) -f *.ll
	$(RM) -f *.ll.2
