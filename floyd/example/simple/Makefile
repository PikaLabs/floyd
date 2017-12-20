CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pipe -fPIC -DNDEBUG  -DLOG_LEVEL=LEVEL_INFO -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
else
	CXXFLAGS = -pg -g -O2 -ggdb3 -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX 
endif

ifndef SLASH_PATH
  $(warning Warning: slash path missing, using default)
	SLASH_PATH=$(CURDIR)/third/slash
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a

ifndef PINK_PATH
  $(warning Warning: slash path missing, using default)
	PINK_PATH=$(CURDIR)/third/pink
endif
PINK_INCLUDE_DIR=$(PINK_PATH)
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink.a

ifndef ROCKSDB_PATH
  $(warning Warning: rocksdb path missing, using default)
  ROCKSDB_PATH=$(CURDIR)/third/rocksdb
endif
ROCKSDB_INCLUDE_DIR=$(ROCKSDB_PATH)/include
ROCKSDB_LIBRARY=$(ROCKSDB_PATH)/librocksdb.a

FLOYD_LIBRARY=../../lib/libfloyd.a

CXXFLAGS+=-I../../.. \
					-I$(SLASH_INCLUDE_DIR) \
					-I$(PINK_INCLUDE_DIR) \
					-I$(ROCKSDB_INCLUDE_DIR)

OBJECT = t t1 t2 t3 t4 t5 t6 t7 t8 test_lock test_lock1 test_lock2 add_server add_server1 remove_server read_bench
SRC_DIR = ./
THIRD_PATH = ../../third
OUTPUT = ./output

LIB_PATH = -L../../lib/ \
			 -L$(THIRD_PATH)/slash/slash/lib/ \
			 -L$(THIRD_PATH)/rocksdb/ \
			 -L$(THIRD_PATH)/pink/pink/lib/


LIBS = -lfloyd \
			 -lpink \
			 -lslash \
			 -lrocksdb \
			 -lsnappy \
			 -lprotobuf \
			 -lz \
			 -lbz2 \
			 -lrt \
			 -lssl \
			 -lcrypto \
			 -lpthread

INCLUDE_PATH = -I../../../ \
							 -I$(THIRD_PATH)/rocksdb/include \
							 -I$(THIRD_PATH)/slash/ \
							 -I$(THIRD_PATH)/pink/

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

t: t.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

t1: t1.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

t2: t2.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
t3: t3.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
t4: t4.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
t5: t5.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
t6: t6.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
t7: t7.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
t8: t8.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
test_lock: test_lock.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
test_lock1: test_lock1.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
test_lock2: test_lock2.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
add_server: add_server.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
add_server1: add_server1.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
remove_server: remove_server.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
read_bench: read_bench.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(OUTPUT)
	rm -rf $(SRC_DIR)/log
	rm -rf ./data*
	rm -f $(SRC_DIR)/*.o
	rm -rf $(OBJECT) 
