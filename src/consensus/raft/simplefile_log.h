#ifndef __FLOYD_CONSENSUS_SIMPLEFILE_LOG_H__
#define __FLOYD_CONSENSUS_SIMPLEFILE_LOG_H__

#include "simplelog_meta.pb.h"
#include "memory_log.h"

namespace floyd {
namespace raft {

class SimpleFileLog : public Log {
	class Sync : public Log::Sync {
	public:
		explicit Sync(uint64_t lastindex);
		void Wait();

		std::vector<std::pair<int, bool>> fds;
	};
public:
	typedef floyd::raft::Entry Entry;

	explicit SimpleFileLog(const std::string& parent_dir);
	~SimpleFileLog();
	std::pair<uint64_t, uint64_t> Append(std::vector<Entry*>& entries);
	std::unique_ptr<Log::Sync> TakeSync();
	void TruncatePrefix(uint64_t first_index);
	void TruncateSuffix(uint64_t last_index);

	Entry& GetEntry(uint64_t index);
	uint64_t GetStartLogIndex();
	uint64_t GetLastLogIndex();
	uint64_t GetSizeBytes();
	void UpdateMetadata();

protected:
	MemoryLog memory_log_;
	floyd::raft::simplefilelog::MetaData metadata_;
	std::unique_ptr<Sync> current_sync_;
	std::string parent_dir_;

	bool ReadMetadata(std::string& filename,
			floyd::raft::simplefilelog::MetaData& metadata);
	std::vector<uint64_t> GetEntryIds();
	Entry Read(std::string& path);
	int ProtocolToFile(google::protobuf::Message& in, std::string& path);
	int FileToProtocol(google::protobuf::Message& out, std::string& path);

};

}
}
#endif
