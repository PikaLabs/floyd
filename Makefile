CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -DLOG_LEVEL=DEBUG -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O0 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
	# CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -D__STDC_FORMAT_MACROS -fPIC -std=c++11 -gdwarf-2
endif
OBJECT = floyd
SRC_DIR = ./src
THIRD_PATH = ./third
OUTPUT = ./output


INCLUDE_PATH = -I./include/ \
			   -I./src/ \
			   -I./src/consensus/ \
			   -I./src/consensus/raft/ \
			   -I$(THIRD_PATH)/leveldb/include/ \
			   -I$(THIRD_PATH)/slash/output/include/ \
			   -I$(THIRD_PATH)/pink/output/include/

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/slash/output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib  \
		   -L$(THIRD_PATH)/leveldb/

LIBS = -lpthread \
	   -lprotobuf \
	   -lleveldb \
	   -lslash \
	   -lpink

LIBRARY = libfloyd.a

.PHONY: all clean

BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
BASE_OBJS += $(wildcard $(SRC_DIR)/consensus/raft/*.cc)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

LIBSLASH = $(THIRD_PATH)/slash/output/lib/libslash.a
LIBPINK = $(THIRD_PATH)/pink/output/lib/libpink.a
LIBLEVELDB = $(THIRD_PATH)/leveldb/libleveldb.a
$(LIBRARY):  $(LIBPINK) 
all: $(LIBRARY)
	@echo "Success, go, go, go..."

$(LIBSLASH):
	make -C $(THIRD_PATH)/slash/

$(LIBPINK):
	make -C $(THIRD_PATH)/pink/

$(LIBLEVELDB): 
	make -C $(THIRD_PATH)/leveldb/

$(LIBRARY): $(LIBSLASH) $(LIBPINK) $(OBJS) $(LIBLEVELDB)
	make -C third/pink 
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/include
	mkdir $(OUTPUT)/lib
	rm -rf $@
	ar -rcs $@ $(OBJS)
	cp -r ./include $(OUTPUT)/
	cp -r ./src/command.pb.h $(OUTPUT)/include
	cp -r ./src/meta.pb.h $(OUTPUT)/include
	mkdir $(OUTPUT)/include/raft
	cp -r ./src/consensus/raft/*.h $(OUTPUT)/include/raft
	mv $@ $(OUTPUT)/lib/
	make -C example __PERF=$(__PERF)
	make -C test __PERF=$(__PERF)

$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	make clean -C example
	make clean -C test
	make clean -C third/pink/
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(SRC_DIR)/consensus/raft/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
