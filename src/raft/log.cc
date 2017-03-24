#include "log.h"

namespace floyd {
namespace raft {

Log::Log() : metadata() {}

Log::~Log() {}

Log::Sync::Sync(uint64_t lastindex) : last_index(lastindex), completed(false) {}

Log::Sync::~Sync() {}
}
}
