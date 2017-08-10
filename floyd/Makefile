CLEAN_FILES = # deliberately empty, so we can append below.
CC=cc
CXX=g++
LDFLAGS= -lpthread -lrt -lprotobuf
CFLAGS= -fno-builtin-memcmp -msse -msse4.2
CXXFLAGS= -std=c++11 -fno-builtin-memcmp -msse -msse4.2
PROFILING_FLAGS=-pg
ARFLAGS = rs
OPT=

# Set the default DEBUG_LEVEL to 0
DEBUG_LEVEL?=0

ifeq ($(MAKECMDGOALS),dbg)
  DEBUG_LEVEL=2 # compatible with rocksdb
endif

# compile with -O2 if for release
# if we're compiling for release, compile without debug code (-DNDEBUG) and
# don't treat warnings as errors
ifeq ($(DEBUG_LEVEL),0)
DISABLE_WARNING_AS_ERROR=1
OPT += -O2 -fno-omit-frame-pointer -DNDEBUG
OPT += -DLOG_LEVEL=LEVEL_INFO
else
$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
OPT += -O0 -D__XDEBUG__ -DLOG_LEVEL=LEVEL_DEBUG $(PROFILING_FLAGS)
endif

# for rocksdb
OPT += -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX

#-----------------------------------------------

SRC_DIR=./src
LIB_SOURCES := $(SRC_DIR)/build_version.cc \
				$(wildcard $(SRC_DIR)/*.cc) \
				$(wildcard $(SRC_DIR)/*.c) \

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

CFLAGS += -g
CXXFLAGS += -g

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  -Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

CFLAGS += $(WARNING_FLAGS) -I.. -I$(SLASH_INCLUDE_DIR) -I$(PINK_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR) $(OPT)
CXXFLAGS += $(WARNING_FLAGS) -I.. -I$(SLASH_INCLUDE_DIR) -I$(PINK_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR) $(OPT) \
						-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

date := $(shell date +%F)
git_sha := $(shell git rev-parse HEAD 2>/dev/null)
gen_build_version = sed -e s/@@GIT_SHA@@/$(git_sha)/ -e s/@@GIT_DATE_TIME@@/$(date)/ $(SRC_DIR)/build_version.cc.in
# Record the version of the source that we are compiling.
# We keep a record of the git revision in this file.  It is then built
# as a regular source file as part of the compilation process.
# One can run "strings executable_filename | grep _build_" to find
# the version of the source that we used to build the executable file.
CLEAN_FILES += $(SRC_DIR)/build_version.cc

$(SRC_DIR)/build_version.cc: FORCE
	$(AM_V_GEN)rm -f $@-t
	$(AM_V_at)$(gen_build_version) > $@-t
	$(AM_V_at)if test -f $@; then         \
	  cmp -s $@-t $@ && rm -f $@-t || mv -f $@-t $@;    \
	else mv -f $@-t $@; fi
FORCE: 

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

# if user didn't config LIBNAME, set the default
ifeq ($(LIBNAME),)
# we should only run floyd in production with DEBUG_LEVEL 0
ifeq ($(DEBUG_LEVEL),0)
        LIBNAME=libfloyd
else
        LIBNAME=libfloyd
endif
endif
LIBOUTPUT = ./lib
dummy := $(shell mkdir -p $(LIBOUTPUT))
LIBRARY = $(LIBOUTPUT)/${LIBNAME}.a

.PHONY: clean dbg static_lib all

all: $(LIBRARY)

static_lib: $(LIBRARY)

dbg: $(LIBRARY)

$(LIBRARY): $(LIBOBJECTS)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $(LIBOBJECTS)

clean:
	rm -f $(TESTS)
	rm -f $(LIBRARY)
	rm -rf $(CLEAN_FILES)
	rm -rf $(LIBOUTPUT)
	find . -name "*.[oda]*" ! -path "./third/*" -exec rm -f {} \;

.cc.o:
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
