CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O2 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
	# CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -D__STDC_FORMAT_MACROS -fPIC -std=c++11 -gdwarf-2
endif

OBJECT = test
SRC_DIR = ./
TEST_SRC_DIR = ../src
THIRD_PATH = ../third
OUTPUT = ./output

INCLUDE_PATH = -I../include/ \
                     -I../src/ \
			   -I$(THIRD_PATH)/leveldb/include/ \
			   -I$(THIRD_PATH)/slash/include/ \
			   -I$(THIRD_PATH)/slash/ \
			   -I$(THIRD_PATH)/pink/include/ \
			   -I$(THIRD_PATH)/pink/

LIB_PATH = -L../ \
		   -L../output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib/  \
		   -L$(THIRD_PATH)/slash/output/lib/  \
		   -L$(THIRD_PATH)/leveldb/

LIBS = -lpthread \
	   -lprotobuf \
	   -lpink \
	   -lslash \
	   -lfloyd \
	   -lleveldb


LIBRARY = libfloyd.a

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

TEST_BASE_OBJS := $(wildcard $(TEST_SRC_DIR)/*.cc)
TEST_BASE_OBJS += $(wildcard $(TEST_SRC_DIR)/*.c)
TEST_BASE_OBJS += $(wildcard $(TESTT_SRC_DIR)/*.cpp)
TEST_OBJS = $(patsubst %.cc,%.o,$(TEST_BASE_OBJS))

LIBLEVELDB = $(THIRD_PTATH)/leveldb/libleveldb.a
all: $(OBJECT) 
	@echo "Success, go, go, go..."


$(OBJECT): $(OBJS) 
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS)

$(OBJS): %.o : %.cc $(TEST_OBJS)
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TEST_OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)*.o
	rm -rf $(OBJECT)
	rm -rf log
