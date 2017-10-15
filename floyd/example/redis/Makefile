CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -DNDEBUG  -DLOG_LEVEL=LEVEL_INFO -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
else
	CXXFLAGS = -g -O2 -ggdb3 -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX 
endif

ifndef SLASH_PATH
  $(warning Warning: slash path missing, using default)
	SLASH_PATH=$(CURDIR)/../../third/slash
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a

ifndef PINK_PATH
  $(warning Warning: slash path missing, using default)
	PINK_PATH=$(CURDIR)/../../third/pink
endif
PINK_INCLUDE_DIR=$(PINK_PATH)
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink.a

ifndef ROCKSDB_PATH
  $(warning Warning: rocksdb path missing, using default)
  ROCKSDB_PATH=$(CURDIR)/../../third/rocksdb
endif
ROCKSDB_INCLUDE_DIR=$(ROCKSDB_PATH)/include
ROCKSDB_LIBRARY=$(ROCKSDB_PATH)/librocksdb.a

FLOYD_LIBRARY=../../lib/libfloyd.a

CXXFLAGS+=-I../../.. \
					-I$(SLASH_INCLUDE_DIR) \
					-I$(PINK_INCLUDE_DIR) \
					-I$(ROCKSDB_INCLUDE_DIR)

OBJECT = raftis
SRC_DIR = ./
THIRD_PATH = ../../third
OUTPUT = ./output

LIB_PATH = -L../../lib/ \
			 -L$(SLASH_PATH)/slash/lib/ \
			 -L$(ROCKSDB_PATH)/ \
			 -L$(PINK_PATH)/pink/lib/


LIBS = -lfloyd \
			 -lpink \
			 -lslash \
			 -lrocksdb \
			 -lsnappy \
			 -lprotobuf \
			 -lz \
			 -lbz2 \
			 -lrt \
			 -lpthread

INCLUDE_PATH = -I../../../ \
							 -I$(THIRD_PATH)/rocksdb/include \
							 -I$(THIRD_PATH)/slash/ \
							 -I$(THIRD_PATH)/pink/

AM_DEFAULT_VERBOSITY = 0

AM_V_GEN = $(am__v_GEN_$(V))
am__v_GEN_ = $(am__v_GEN_$(AM_DEFAULT_VERBOSITY))
am__v_GEN_0 = @echo "  GEN     " $@;
am__v_GEN_1 =
AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =

AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC      " $@;
am__v_CC_1 =
CCLD = $(CC)
LINK = $(CCLD) $(AM_CFLAGS) $(CFLAGS) $(AM_LDFLAGS) $(LDFLAGS) -o $@
AM_V_CCLD = $(am__v_CCLD_$(V))
am__v_CCLD_ = $(am__v_CCLD_$(AM_DEFAULT_VERBOSITY))
am__v_CCLD_0 = @echo "  CCLD    " $@;
am__v_CCLD_1 =
AM_V_AR = $(am__v_AR_$(V))
am__v_AR_ = $(am__v_AR_$(AM_DEFAULT_VERBOSITY))
am__v_AR_0 = @echo "  AR      " $@;
am__v_AR_1 =

AM_LINK = $(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)


.PHONY: all clean version


BASE_BOJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_BOJS += $(wildcard $(SRC_DIR)/*.c)
BASE_BOJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_BOJS))

all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir -p $(OUTPUT)
	mkdir -p $(OUTPUT)/bin
	cp $(OBJECT) $(OUTPUT)/bin/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."

raftis: raftis.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

.cc.o:
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

clean: 
	rm -rf $(OUTPUT)
	rm -rf $(SRC_DIR)/log
	rm -rf ./data*
	rm -f $(SRC_DIR)/*.o
	rm -rf $(OBJECT) 
