#ifndef __FLOYD_CONSENSUS_STATE_MACHINE_H__
#define __FLOYD_CONSENSUS_STATE_MACHINE_H__

#include <unordered_map>
#include "log.h"
#include "status.h"
#include "floyd_mutex.h"
#include "floyd_define.h"

#include "pink_thread.h"

namespace floyd {
namespace raft {

class RaftConsensus;

class StateMachine {
public:
	struct Entry {
		uint64_t index;
		Log::Entry log_entry;
	};
  struct ApplyResult {
    floyd::Status status_;
    std::string value_;

    ApplyResult() {}
    ApplyResult(floyd::Status status, std::string value){
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
  floyd::Status WaitForTryLockResponse(uint64_t log_index);
  floyd::Status WaitForUnLockResponse(uint64_t log_index);
  floyd::Status WaitForDeleteUserResponse(uint64_t log_index);

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
  //floyd::Status DBLock(std::string &key, std::string &user);

	RaftConsensus* raft_con_;
	mutable Mutex mutex_;
	mutable CondVar apply_state_;
	uint64_t last_apply_index_;
  bool exiting_;
};

}
}

#endif
