RPATH = ./output/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -DLOG_LEVEL=LEVEL_DEBUG -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
endif
OBJECT = log_parser
SRC_DIR = ./
THIRD_PATH = ../third

ifndef SLASH_PATH
SLASH_PATH = $(realpath $(THIRD_PATH)/slash)
endif

ifndef PINK_PATH
PINK_PATH = $(realpath $(THIRD_PATH)/pink)
endif


INCLUDE_PATH = -I./ \
							 -I$(SLASH_PATH) \
							 -I$(PINK_PATH)

LIB_PATH = -L./ \
			 -L$(SLASH_PATH)/slash/lib/ \
			 -L$(PINK_PATH)/pink/lib/

LIBS = -lpthread \
	   -lprotobuf \
	   -lslash \
	   -lpink

.PHONY: all clean

BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

LIBSLASH = $(SLASH_PATH)/slash/lib/libslash.a
LIBPINK = $(PINK_PATH)/pink/lib/libpink.a

all: $(OBJECT)
	@echo " OBJS : $(OBJS)"
	@echo "Success, go, go, go..."

$(LIBSLASH):
	make -C $(SLASH_PATH)/slash/

$(LIBPINK):
	make -C $(PINK_PATH)/pink/

$(OBJECT): $(LIBSLASH) $(LIBPINK) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) -Wl,-Bdynamic $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)
