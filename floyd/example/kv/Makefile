CC=cc
CXX=g++
LDFLAGS= -lsnappy -lz -lbz2 -lprotobuf -lpthread -lrt $(EXTRA_LDFLAGS)
CFLAGS= -DOS_LINUX -fno-builtin-memcmp -msse -msse4.2 ${EXTRA_CFLAGS}
CXXFLAGS=-std=c++11 -DOS_LINUX -fno-builtin-memcmp -msse -msse4.2 ${EXTRA_CXXFLAGS}
# OPT = -O0 -g
OPT = -O2 -DNDEBUG

# for rocksdb
CXXFLAGS+= -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX

.PHONY: clean all

all: srv cli

ifndef SLASH_PATH
  $(warning Warning: missing slash path, using default)
	SLASH_PATH=$(CURDIR)/../third/slash
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

srv: kv_server.cc srv.cc $(FLOYD_LIBRARY) $(ROCKSDB_LIBRARY) $(PINK_LIBRARY) $(SLASH_LIBRARY)
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

cli: kv_cli.cc cli.cc $(FLOYD_LIBRARY) $(ROCKSDB_LIBRARY) $(PINK_LIBRARY) $(SLASH_LIBRARY)
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./srv ./cli

$(FLOYD_LIBRARY):
	cd ../.. && $(MAKE) static_lib
