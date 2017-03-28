CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -DLOG_LEVEL=LEVEL_DEBUG -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O2 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
	# CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -D__STDC_FORMAT_MACROS -fPIC -std=c++11 -gdwarf-2
endif

ifndef SLASH_PATH
SLASH_PATH = ./third/slash
endif

ifndef PINK_PATH
PINK_PATH = ./third/pink
endif

ifndef NEMODB_PATH
NEMODB_PATH = ./third/nemo-rocksdb
endif

OBJECT = floyd
SRC_DIR = ./src
LIB_OUTPUT = ./lib


INCLUDE_PATH = -I../ \
							 -I$(SLASH_PATH)/ \
							 -I$(PINK_PATH)/ \
							 -I$(NEMODB_PATH)/include/ \
							 -I$(NEMODB_PATH)/rocksdb/ \
							 -I$(NEMODB_PATH)/rocksdb/include

LIB_PATH = -L./ \
		   -L$(SLASH_PATH)/slash/lib/ \
		   -L$(PINK_PATH)/pink/lib/  \
		   -L$(NEMODB_PATH)/lib/ \
		   -L$(NEMODB_PATH)/rocksdb/ 

LIBS = -lpthread \
	   -lprotobuf \
	   -lslash \
	   -lpink \
	   -lnemodb \
	   -lrocksdb

LIBRARY = libfloyd.a

.PHONY: all clean distclean
BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
BASE_OBJS += $(wildcard $(SRC_DIR)/raft/*.cc)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

LIBSLASH = $(SLASH_PATH)/slash/lib/libslash.a
LIBPINK = $(PINK_PATH)/pink/lib/libpink.a
LIBNEMODB= $(NEMODB_PATH)/nemo-rocksdb/lib/libnemodb.a

all: $(LIBRARY)
	@echo "Success, go, go, go..."

$(LIBSLASH):
	make -C $(SLASH_PATH)/slash __PERF=$(__PERF)

$(LIBPINK):
	make -C $(PINK_PATH)/pink __PERF=$(__PERF)  SLASH_PATH=../../../$(SLASH_PATH)

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

