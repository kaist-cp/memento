_HOSTNAME := $(shell hostname)
MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
DIR_PATH := $(patsubst %/,%,$(dir $(MKFILE_PATH)))
CURRENT_DIR := $(notdir $(patsubst %/,%,$(dir $(MKFILE_PATH))))

LIBTXLOCK_DIR=$(DIR_PATH)
#LIBTXLOCK_CFLAGS=-isystem $(LIBTXLOCK_DIR)/include -I$(LIBTXLOCK_DIR) -pthread
LIBTXLOCK_CFLAGS= -I$(LIBTXLOCK_DIR) -pthread
LIBTXLOCK_CCFLAGS=$(LIBTXLOCK_CFLAGS) -pthread
LIBTXLOCK_CXXFLAGS=$(LIBTXLOCK_CCFLAGS) -pthread
LIBTXLOCK_LDFLAGS=-L$(LIBTXLOCK_DIR) -pthread -Wl,-rpath -Wl,$(LIBTXLOCK_DIR) -ltxlock -lrt -ldl
