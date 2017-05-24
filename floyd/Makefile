CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -ggdb3 -pg -pipe -fPIC -DLOG_LEVEL=LEVEL_DEBUG -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
else
	CXXFLAGS = -O2 -g -pipe -fPIC -DNDEBUG -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
endif

ifndef SLASH_PATH
SLASH_PATH = $(realpath ./third/slash)
endif

ifndef PINK_PATH
PINK_PATH = $(realpath ./third/pink)
endif

ifndef NEMODB_PATH
NEMODB_PATH = $(realpath ./third/nemo-rocksdb)
endif

OBJECT = floyd
SRC_DIR = ./src
LIB_OUTPUT = ./lib


INCLUDE_PATH = -I../ \
							 -I$(SLASH_PATH)/ \
							 -I$(PINK_PATH)/ \
							 -I$(NEMODB_PATH)/output/include/ \
							 -I$(NEMODB_PATH)/rocksdb/ \
							 -I$(NEMODB_PATH)/rocksdb/include

LIB_PATH = -L./ \
		   -L$(SLASH_PATH)/slash/lib/ \
		   -L$(PINK_PATH)/pink/lib/  \
		   -L$(NEMODB_PATH)/output/lib/

LIBS = -lpthread \
	   -lprotobuf \
	   -lpink \
	   -lnemodb \
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
LIBNEMODB= $(NEMODB_PATH)/nemo-rocksdb/output/lib/libnemodb.a

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

$(LIBNEMODB): 
	make -C $(NEMODB_PATH)/ __PERF=$(__PERF)

$(LIBRARY): $(LIBSLASH) $(LIBPINK) $(LIBNEMODB) $(LIBOBJS) $(OBJS)
	rm -rf $(LIB_OUTPUT)
	mkdir $(LIB_OUTPUT)
	rm -rf $@
	ar -rcs $@ $(OBJS)
	mv $@ $(LIB_OUTPUT)/

$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(LIB_OUTPUT)/*

distclean: clean
	make clean -C $(PINK_PATH)/pink distclean
	make clean -C $(SLASH_PATH)/slash
	make clean -C $(NEMODB_PATH)/ distclean

