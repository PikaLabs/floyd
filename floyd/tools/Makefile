CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -DLOG_LEVEL=LEVEL_DEBUG -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O0 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
	# CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -D__STDC_FORMAT_MACROS -fPIC -std=c++11 -gdwarf-2
endif
OBJECT = log_parser
SRC_DIR = ./
THIRD_PATH = ../third


INCLUDE_PATH = -I./include/ \
			   -I$(THIRD_PATH)/slash/output/include/ \
			   -I$(THIRD_PATH)/pink/output/include/ \
			   -I$(THIRD_PATH)/pink/output/

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/slash/output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib

LIBS = -lpthread \
	   -lprotobuf \
	   -lslash \
	   -lpink

.PHONY: all clean

BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

LIBSLASH = $(THIRD_PATH)/slash/output/lib/libslash.a
LIBPINK = $(THIRD_PATH)/pink/output/lib/libpink.a
LIBLEVELDB = $(THIRD_PATH)/leveldb/out-static/libleveldb.a

all: $(OBJECT)
	@echo " OBJS : $(OBJS)"
	@echo "Success, go, go, go..."

$(LIBSLASH):
	make -C $(THIRD_PATH)/slash/

$(LIBPINK):
	make -C $(THIRD_PATH)/pink/

$(LIBLEVELDB): 
	make -C $(THIRD_PATH)/leveldb/

$(OBJECT): $(LIBSLASH) $(LIBPINK) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)
