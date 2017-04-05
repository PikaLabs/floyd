#ifndef __FLOYD_CONSENSUS_STATE_MACHINE_H__
#define __FLOYD_CONSENSUS_STATE_MACHINE_H__

#include <unordered_map>
#include "log.h"
#include "floyd_mutex.h"
#include "floyd_define.h"

#include "include/pink_thread.h"

#include "slash_status.h"

using slash::Status;

namespace floyd {
namespace raft {

using slash::Status;
class RaftConsensus;

class StateMachine {
public:
	struct Entry {
		uint64_t index;
		Log::Entry log_entry;
	};
  struct ApplyResult {
    Status status_;
    std::string value_;

    ApplyResult() {}
    ApplyResult(Status status, std::string value){
      status_ = status;
      value_ = value;
    }
  };
  std::unordered_map<uint64_t, ApplyResult> result_map_;

	StateMachine(RaftConsensus* raft_con);
	~StateMachine();

	void Init();

	bool WaitForReadResponse(uint64_t log_index, std::string& key,std::string& value);
	bool WaitForReadAllResponse(uint64_t log_index,KVMap& kvmap);
	bool WaitForWriteResponse(uint64_t log_index);

	uint64_t last_apply_index() {
    return last_apply_index_;
  };

  Status WaitForTryLockResponse(uint64_t log_index);
  Status WaitForUnLockResponse(uint64_t log_index);
  Status WaitForDeleteUserResponse(uint64_t log_index);

  void Exit();

	friend class ApplyThread;

private:

	class ApplyThread : public pink::Thread {
	public:
		ApplyThread(StateMachine* sm);
		~ApplyThread();

		virtual void* ThreadMain();
		bool Apply(StateMachine::Entry& entry);
	private:
		StateMachine* sm_;
	};
	ApplyThread* apply_;

  //int LockIsAvailable(std::string &key, std::string &user);
  //Status DBLock(std::string &key, std::string &user);

	RaftConsensus* raft_con_;
	mutable Mutex mutex_;
	mutable CondVar apply_state_;
	uint64_t last_apply_index_;
  bool exiting_;
};

}
}

#endif
