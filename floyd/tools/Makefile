CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pipe -fPIC -DNDEBUG  -DLOG_LEVEL=LEVEL_INFO -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
else
	CXXFLAGS = -pg -O2 -ggdb3 -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-unused-variable -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX 
endif

OBJECT = read_rock read_floyd cpt
SRC_DIR = ./
THIRD_PATH = ../third
OUTPUT = ./output

LIB_PATH = -L../lib/ \
			 -L$(THIRD_PATH)/slash/slash/lib/ \
			 -L$(THIRD_PATH)/rocksdb/ \
			 -L$(THIRD_PATH)/pink/pink/lib/


LIBS = -lpink \
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

INCLUDE_PATH = -I../../ \
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

read_rock: read_rock.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
cpt: cpt.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
read_floyd: read_floyd.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(OUTPUT)
	rm -rf $(SRC_DIR)/log
	rm -rf ./data*
	rm -f $(SRC_DIR)/*.o
	rm -rf $(OBJECT) 
