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
class RaftMeta;
/*
 * we use FloydContext to avoid passing the floyd_impl's this point to other thread
 */
struct FloydContext {
  /* Role related */
  FloydContext(const Options& _options) 
    : options(_options),
      apply_cond(&apply_mu) {};
  void leader_node(std::string* ip, int* port);
  void voted_for_node(std::string* ip, int* port);

  bool HasLeader();

  void RecoverInit(RaftMeta *raft);
  void BecomeFollower(uint64_t new_iterm,
      const std::string leader_ip = "", int port = 0);
  void BecomeCandidate();
  void BecomeLeader();

  void GrantVote(uint64_t term, const std::string ip, const int port);
  bool ReceiverDoAppendEntries(uint64_t term,
      uint64_t pre_log_term, uint64_t pre_log_index,
      std::vector<Entry*>& entries, uint64_t* my_term);
  
  Options options;
  // Role related
  uint64_t current_term;

  Role role;
  std::string voted_for_ip;
  int voted_for_port;
  std::string leader_ip;
  int leader_port;
  uint32_t vote_quorum;

  uint64_t commit_index;
  uint64_t last_applied;
  uint64_t last_op_time;

  // mutex protect commit_index
  // used in floyd_apply thread and floyd_peer thread
  // floyd_apply thread will get the commit_index
  // floyd_peer thread will update the commit_index
  slash::Mutex commit_index_mu;
  slash::Mutex commit_mu;
  slash::Mutex apply_mu;
  slash::CondVar apply_cond;

};

} // namespace floyd
#endif  // FLOYD_SRC_FLOYD_CONTEXT_H_
