#ifndef FLOYD_CONTEXT_H_
#define FLOYD_CONTEXT_H_

#include <pthread.h>

#include "floyd/include/floyd_options.h"
#include "floyd/src/raft/log.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

using slash::Status;

enum Role {
  kFollower = 0,
  kCandidate = 1,
  kLeader = 2,
};

class FloydContext {
 public:
  FloydContext(const Options& opt, Log* log);

  bool Init();
  Log* log() {
    return log_;
  }

  /* Role related */
  std::pair<std::string, int> leader_node() {
    slash::RWLock l(&stat_rw_, false);
    return {leader_ip_, leader_port_};
  }

  uint64_t current_term() {
    slash::RWLock l(&stat_rw_, false);
    return current_term_;
  }

  Role role() {
    slash::RWLock l(&stat_rw_, false);
    return role_;
  }

  std::string local_ip() {
    return options_.local_ip;
  }

  int local_port() {
    return options_.local_port;
  }

  uint64_t heartbeat_us() {
    return options_.heartbeat_us;
  }
  
  void BecomeFollower(uint64_t new_iterm,
      const std::string leader_ip = "", int port = 0);
  void BecomeCandidate();
  void BecomeLeader();
  bool VoteAndCheck(uint64_t vote_term);
  bool RequestVote(uint64_t term,
      const std::string ip, uint32_t port,
      uint64_t log_index, uint64_t log_term,
      uint64_t* my_term);
  bool AppendEntires(uint64_t term,
      uint64_t pre_log_term, uint64_t pre_log_index,
      std::vector<Log::Entry*> entries, uint64_t* my_term);

  /* Commit related */
  void SetCommitIndex(uint64_t commit_index) {
    slash::MutexLock l(&commit_mu_);
    commit_index_ = commit_index;
  }
  
  uint64_t commit_index() {
    slash::MutexLock l(&commit_mu_);
    return commit_index_;
  }
  
  /* Apply related */
  // Return false if timeout
  Status WaitApply(uint64_t commit_index, uint32_t timeout);
  void SignalApply() {
    apply_cond_.SignalAll();
  }
  
  uint64_t NextApplyIndex(uint64_t* len) {
    slash::MutexLock lcommit(&commit_mu_);
    slash::MutexLock lapply(&apply_mu_);
    *len = commit_index_ - apply_index_;
    return apply_index_ + 1;
  }

  uint64_t apply_index() {
    slash::MutexLock lapply(&apply_mu_);
    return apply_index_;
  }

  uint64_t AdvanceCommitIndex(uint64_t* len);

  void ApplyDone(uint64_t index) {
    slash::MutexLock lapply(&apply_mu_);
    apply_index_ = index; 
  }

 private:
  Options options_;
  Log* log_;

  // Role related
  pthread_rwlock_t stat_rw_;
  uint64_t current_term_;
  Role role_;
  std::string voted_for_ip_;
  int voted_for_port_;
  std::string leader_ip_;
  int leader_port_;
  int vote_quorum_;

  // Commit related
  slash::Mutex commit_mu_;
  uint64_t commit_index_;

  // Apply related
  slash::Mutex apply_mu_;
  slash::CondVar apply_cond_;
  uint64_t apply_index_;

  void LogApply();
};

} // namespace floyd
#endif
