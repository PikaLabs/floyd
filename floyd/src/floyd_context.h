// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef FLOYD_SRC_FLOYD_CONTEXT_H_
#define FLOYD_SRC_FLOYD_CONTEXT_H_

#include <pthread.h>

#include "floyd/include/floyd_options.h"
#include "floyd/src/raft_log.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

using slash::Status;

enum Role {
  kFollower = 0,
  kCandidate = 1,
  kLeader = 2,
};

class Logger;
class RaftLog;

class FloydContext {
 public:
  FloydContext(const Options& opt, RaftLog* raft_log, Logger* info_log);
  ~FloydContext();

  void RecoverInit();
  RaftLog* raft_log() {
    return raft_log_;
  }

  Logger* info_log() {
    return info_log_;
  }

  /* Role related */
  void leader_node(std::string* ip, int* port);
  void voted_for_node(std::string* ip, int* port);

  bool HasLeader();

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

  uint64_t GetElectLeaderTimeout();

  uint64_t append_entries_size_once() {
    return options_.append_entries_size_once;
  }
  uint64_t append_entries_count_once() {
    return options_.append_entries_count_once;
  }

  bool single_mode() {
    return options_.single_mode;
  }
  
  void BecomeFollower(uint64_t new_iterm,
      const std::string leader_ip = "", int port = 0);
  void BecomeCandidate();
  void BecomeLeader();

  bool VoteAndCheck(uint64_t vote_term);
  bool ReceiverDoRequestVote(uint64_t term,
      const std::string ip, int port,
      uint64_t log_term, uint64_t log_index,
      uint64_t* my_term);
  bool ReceiverDoAppendEntries(uint64_t term,
      uint64_t pre_log_term, uint64_t pre_log_index,
      std::vector<Entry*>& entries, uint64_t* my_term);

  /* Commit related */
  uint64_t commit_index() {
    slash::MutexLock l(&commit_mu_);
    return commit_index_;
  }
  void set_commit_index(uint64_t commit_index) {
    commit_index_ = commit_index;
  }
  bool AdvanceLeaderCommitIndex(uint64_t leader_commit);
  bool AdvanceFollowerCommitIndex(uint64_t leader_commit);
  
  /* Apply related */
  // Return false if timeout
  Status WaitApply(uint64_t last_applied, uint32_t timeout);
  
  // commit index may be smaller than apply index,
  // so we should check len first;
  uint64_t NextApplyIndex(uint64_t* len);

  uint64_t last_applied() {
    slash::MutexLock lapply(&apply_mu_);
    return last_applied_;
  }

  void ApplyDone(uint64_t index);
  void MetaApply();
  
  // Commit related
  slash::Mutex commit_mu_;
  slash::Mutex apply_mu_;

  pthread_rwlock_t stat_rw_;

 private:
  Options options_;
  RaftLog* raft_log_;
  
  // used to debug
  Logger* info_log_;

  // Role related
  uint64_t current_term_;

  Role role_;
  std::string voted_for_ip_;
  int voted_for_port_;
  std::string leader_ip_;
  int leader_port_;
  uint32_t vote_quorum_;

  uint64_t commit_index_;

  // Apply related
  slash::CondVar apply_cond_;
  uint64_t last_applied_;

};

} // namespace floyd
#endif  // FLOYD_SRC_FLOYD_CONTEXT_H_
