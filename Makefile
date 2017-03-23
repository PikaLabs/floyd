CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -DLOG_LEVEL=LEVEL_DEBUG -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
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
			   -I./src/raft/ \
			   -I$(THIRD_PATH)/leveldb/include/ \
			   -I$(THIRD_PATH)/slash/output/include/ \
			   -I$(THIRD_PATH)/pink/output/include/ \
			   -I$(THIRD_PATH)/pink/output/

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
BASE_OBJS += $(wildcard $(SRC_DIR)/raft/*.cc)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

LIBSLASH = $(THIRD_PATH)/slash/output/lib/libslash.a
LIBPINK = $(THIRD_PATH)/pink/output/lib/libpink.a
LIBLEVELDB = $(THIRD_PATH)/leveldb/out-static/libleveldb.a
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
	mkdir $(OUTPUT)/include/raft
	cp -r ./src/raft/*.h $(OUTPUT)/include/raft
	mv $@ $(OUTPUT)/lib/
	#make -C sdk __PERF=$(__PERF)
	#make -C server __PERF=$(__PERF)
	cp -r $(THIRD_PATH)/leveldb/include/leveldb $(OUTPUT)/include/
	cp $(LIBLEVELDB) $(OUTPUT)/lib/

$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(SRC_DIR)/raft/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)

distclean: clean
	make clean -C $(THIRD_PATH)/pink
	make clean -C $(THIRD_PATH)/slash
	make clean -C example/sdk
	make clean -C example/server

