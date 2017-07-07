RPATH = ./output/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

CXX = g++
ifeq ($(__REL), 1)
	CXXFLAGS = -O0 -g -pipe -fPIC -DNDEBUG  -DLOG_LEVEL=LEVEL_INFO -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
else
	CXXFLAGS = -pg -O0 -ggdb3 -pipe -fPIC -W -DLOG_LEVEL=LEVEL_DEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX 
endif

OBJECT = srv cli
SRC_DIR = ./
THIRD_PATH = ../../third
OUTPUT = ./output

LIB_PATH = -L../../lib/ \
			 -L$(THIRD_PATH)/slash/slash/lib/ \
			 -L$(THIRD_PATH)/rocksdb/ \
			 -L$(THIRD_PATH)/pink/pink/lib/


LIBS = -lfloyd \
			 -lpink \
			 -lssl \
			 -lcrypto \
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
	#cp -rf ./lib $(OUTPUT)/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."

srv: kv_server.o srv.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

srv: kv_cli.o cli.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(OUTPUT)
	rm -rf $(SRC_DIR)/log
	rm -f $(SRC_DIR)/*.o
	rm -rf $(OBJECT) 
