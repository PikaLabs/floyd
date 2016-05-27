#ifndef __FLOYD_CONSENSUS_RAFT_LOG_H__
#define __FLOYD_CONSENSUS_RAFT_LOG_H__

#include "floyd_util.h"
#include "raft.pb.h"
#include "log_meta.pb.h"

namespace floyd {

namespace raft {

/** 
 * This interface is used by Raft to store log entries and metadata.
 */
class Log {
public:
	class Sync {
	public:
		explicit Sync(uint64_t lastindex);
		virtual ~Sync();

		virtual void Wait() {}
		uint64_t last_index;
		bool completed;
	};

	typedef floyd::raft::Entry Entry;

	Log();
	virtual ~Log();

	/**
	 * Start to append new entried to log.
	 */
	virtual std::pair<uint64_t, uint64_t> Append(
			std::vector<Entry*>& entries) = 0;
	virtual void UpdateMetadata() = 0;
	virtual std::unique_ptr<Sync> TakeSync() = 0;
	
	virtual Entry& GetEntry(uint64_t index) = 0;
	virtual uint64_t GetStartLogIndex() = 0;
	virtual uint64_t GetLastLogIndex() = 0;
	virtual uint64_t GetSizeBytes() = 0;
	virtual void TruncatePrefix(uint64_t first_index) = 0;
	virtual void TruncateSuffix(uint64_t last_index) = 0;

	void SyncComplete(std::unique_ptr<Sync> sync) {
		sync->completed = true;
		std::move(sync);
	}

	floyd::raft::log::MetaData metadata;
};

}
}

#endif
