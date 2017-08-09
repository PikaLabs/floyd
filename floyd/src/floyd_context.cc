// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_context.h"

#include <stdlib.h>

#include "slash/include/env.h"
#include "slash/include/xdebug.h"

#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"
#include "floyd/src/raft_meta.h"

namespace floyd {

void FloydContext::RecoverInit(RaftMeta *raft_meta) {
  current_term = raft_meta->GetCurrentTerm();
  voted_for_ip = raft_meta->GetVotedForIp();
  voted_for_port = raft_meta->GetVotedForPort();
  commit_index = raft_meta->GetCommitIndex();
  last_applied = raft_meta->GetLastApplied();
  role = Role::kFollower;
}

void FloydContext::BecomeFollower(uint64_t new_term,
                                  const std::string _leader_ip, int _leader_port) {
  // when requestvote receive a large term, then we transfer from candidate to follower
  // then we should set voted_for_ip to the leader_ip
  // if (current_term < new_term) {
  voted_for_ip = _leader_ip;
  voted_for_port = _leader_port;
  // }
  current_term = new_term;
  leader_ip = _leader_ip;
  leader_port = _leader_port;
  role = Role::kFollower;
}

void FloydContext::BecomeCandidate() {
  current_term++;
  role = Role::kCandidate;
  leader_ip.clear();
  leader_port = 0;
  voted_for_ip = options.local_ip;
  voted_for_port = options.local_port;
  vote_quorum = 1;
}

void FloydContext::BecomeLeader() {
  role = Role::kLeader;
  leader_ip = options.local_ip;
  leader_port = options.local_port;
}
}  // namespace floyd
