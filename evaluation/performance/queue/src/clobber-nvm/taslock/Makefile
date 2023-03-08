include config.mk

CFLAGS = -g -std=c11 -O3 -mrtm $(LIBTXLOCK_CFLAGS) -D_POSIX_C_SOURCE=200112L -DUSE_PTHREAD_COND_VARS



all: tl-pthread-mutex.so tl-pthread-wrlock.so libtxlock.so libtxlock.a

libtxlock.so: txlock.s.o txcond.s.o txutil.s.o pthread_cond.s.o
	gcc -shared $^ -ldl -o $@

libtxlock.a: txlock.o txcond.o txutil.o pthread_cond.o
	gcc-ar rcs $@ $^

tl-pthread-mutex.so: tl-pthread-mutex.s.o txlock.s.o txcond.s.o txutil.s.o pthread_cond.s.o
	gcc -g -flto -shared $^ -ldl -o $@

tl-pthread-wrlock.so: tl-pthread-wrlock.s.o txlock.s.o txcond.s.o txutil.s.o pthread_cond.s.o
	gcc -g -flto -shared $^ -ldl -o $@

%.s.o: %.c txlock.h txutil.h txcond.h
	gcc $(CFLAGS) -fPIC -flto -c $< -o $@

%.o: %.c txlock.h txutil.h txcond.h
	gcc $(CFLAGS) -c -flto $< -o $@

clean:
	$(RM) *.o *.so *.a
