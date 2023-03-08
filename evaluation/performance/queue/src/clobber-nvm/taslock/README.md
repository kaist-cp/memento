# HTM Prefetching for Locks

## Idea

## Compilation

## Use of the libraries

### libtxlock.[a|so]

If a program has its own lock implementation, you can replace lock/unlock with
`tl_*`, `tc_*` functions in `txlock.h`.

`txlock` itself is an abstract lock. By setting `LIBTXLOCK` env variable, you
can specify the type of lock you want to use internally for txlock. Current
options (in `txlock.c`) are:

- `tas`: basic tatas lock. It's the default choice if `LIBTXLOCK` is not set.
- `tas_tm`: tatas lock with prefetching
- `ticket` & `ticket_tm`: ticket lock and its prefetching version
- `pthread` & `pthread_tm`: system pthread lock and its prefetching version

For example:
```bash
export LIBTXLOCK=tas_tm
appA # use tas_tm lock for appA
export LIBTXLOCK=pthread
appB # use pthread lock for appB
```
### tl-pthread.so

Assuming `app.bin` is a program compiled with default pthread library, running
`app.bin` with command line: `LD_PRELOAD=path/tl-pthread.so app.bin args` will
dynamicly replace the following pthread functions with ours (in `tl-pthread.h`):

- pthread_mutex_*
- pthread_cond_*

Other pthread functions are not affected.

Again, you can specify the type of txlock via the `LIBTXLOCK` env variable.

## Adding a new lock type

## Benchmarks
