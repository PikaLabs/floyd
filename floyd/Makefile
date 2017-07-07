CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -ggdb3 -pg -pipe -fPIC -D__XDEBUG__ -DLOG_LEVEL=LEVEL_DEBUG -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
else
	CXXFLAGS = -O2 -g -pipe -fPIC -DNDEBUG -W -DLOG_LEVEL=LEVEL_INFO -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
endif

ifndef SLASH_PATH
SLASH_PATH = $(realpath ./third/slash)
endif

ifndef PINK_PATH
PINK_PATH = $(realpath ./third/pink)
endif

ifndef ROCKSDB_PATH
ROCKSDB_PATH = $(realpath ./third/rocksdb)
endif

SRC_DIR = ./src
LIB_OUTPUT = ./lib


INCLUDE_PATH = -I../ \
							 -I$(SLASH_PATH)/ \
							 -I$(PINK_PATH)/ \
							 -I$(ROCKSDB_PATH)/include

LIB_PATH = -L./ \
		   -L$(SLASH_PATH)/slash/lib/ \
		   -L$(PINK_PATH)/pink/lib/  \
		   -L$(ROCKSDB_PATH)/

LIBS = -lpthread \
	   -lprotobuf \
	   -lpink \
	   -lslash \
	   -lrocksdb

LIBRARY = libfloyd.a

.PHONY: all clean distclean test
BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

LIBSLASH = $(SLASH_PATH)/slash/lib/libslash.a
LIBPINK = $(PINK_PATH)/pink/lib/libpink.a
LIBROCKSDB= $(ROCKSDB_PATH)/librocksdb.a

all: $(LIBRARY)
	@echo "Success, go, go, go..."

test: all
	make -C example/server clean
	make -C example/server
	make -C example/sdk clean
	make -C example/sdk

$(LIBSLASH):
	make -C $(SLASH_PATH)/slash __PERF=$(__PERF)

$(LIBPINK):
	make -C $(PINK_PATH)/pink __PERF=$(__PERF)  SLASH_PATH=$(SLASH_PATH)

$(LIBROCKSDB): 
	make -C $(ROCKSDB_PATH)/ static_lib

$(LIBRARY): $(LIBSLASH) $(LIBPINK) $(LIBROCKSDB) $(LIBOBJS) $(OBJS)
	rm -rf $(LIB_OUTPUT)
	mkdir $(LIB_OUTPUT)
	rm -rf $@
	ar -rcs $@ $(OBJS)
	mv $@ $(LIB_OUTPUT)/

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(LIB_OUTPUT)/*

distclean: clean
	make clean -C $(PINK_PATH)/pink 
	make clean -C $(SLASH_PATH)/slash
	make clean -C $(ROCKSDB_PATH)/

