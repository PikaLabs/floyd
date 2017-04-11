#ifndef FLOYD_MEMORY_LOG_H_
#define FLOYD_MEMORY_LOG_H_

#include <deque>

#include "floyd/src/raft/log.h"

namespace floyd {
namespace raft {

class MemoryLog : public Log {
public:
	typedef floyd::raft::Entry Entry;

	MemoryLog();
	~MemoryLog();

	std::pair<uint64_t, uint64_t> Append(std::vector<Entry*>& entries);
	Entry& GetEntry(uint64_t log_index);
	uint64_t GetStartLogIndex();
	uint64_t GetLastLogIndex();
	uint64_t GetSizeBytes();
	std::unique_ptr<Sync> TakeSync();
	void TruncatePrefix(uint64_t first_index);
	void TruncateSuffix(uint64_t last_index);
	void UpdateMetadata();

protected:
	uint64_t start_index_;
	std::deque<Entry> entries_;
	std::unique_ptr<Sync> current_sync_;
};

}
}

#endif
