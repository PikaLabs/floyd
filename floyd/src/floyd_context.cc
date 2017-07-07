// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_context.h"

#include <stdlib.h>
#include "floyd/src/logger.h"


#include "slash/include/env.h"
#include "floyd/src/floyd.pb.h"
#include "slash/include/xdebug.h"

namespace floyd {

FloydContext::FloydContext(const floyd::Options& opt,
                           RaftLog* raft_log, Logger* info_log)
  : options_(opt),
    raft_log_(raft_log),
    info_log_(info_log),
    current_term_(0),
    role_(Role::kFollower),
    voted_for_port_(0),
    leader_port_(0),
    vote_quorum_(0),
    commit_index_(0),
    apply_cond_(&apply_mu_),
    last_applied_(0) {
  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&stat_rw_, &attr);
  uint64_t seed = slash::NowMicros() % 100000;
  LOGV(INFO_LEVEL, info_log_, "ElectLeader srandom with seed %lu", seed);
  srandom(seed);
  //srandom(slash::NowMicros());
}

FloydContext::~FloydContext() {
  pthread_rwlock_destroy(&stat_rw_);
  LOGV(DEBUG_LEVEL, info_log_, "FloydConext dtor");
}

void FloydContext::RecoverInit() {
  assert(raft_log_ != NULL);
  slash::RWLock l(&stat_rw_, true);
  current_term_ = raft_log_->current_term();
  voted_for_ip_ = raft_log_->voted_for_ip();
  voted_for_port_ = raft_log_->voted_for_port();
  last_applied_ = raft_log_->last_applied();
  role_ = Role::kFollower;
}

bool FloydContext::HasLeader() {
  if (leader_ip_ == "" || leader_port_ == 0) {
    return false;
  }
  return true;
}

void FloydContext::leader_node(std::string* ip, int* port) {
  slash::RWLock l(&stat_rw_, false);
  *ip = leader_ip_;
  *port = leader_port_;
}

void FloydContext::voted_for_node(std::string* ip, int* port) {
  slash::RWLock l(&stat_rw_, false);
  *ip = voted_for_ip_;
  *port = voted_for_port_;
}

uint64_t FloydContext::GetElectLeaderTimeout() {
  return rand() % (options_.elect_timeout_ms * 2) + options_.elect_timeout_ms;
}

void FloydContext::BecomeFollower(uint64_t new_term,
                                  const std::string leader_ip, int leader_port) {
  slash::RWLock l(&stat_rw_, true);
  LOGV(DEBUG_LEVEL, info_log_, "BecomeFollower: with current_term_(%lu) and new_term(%lu)"
       " commit_index(%lu)  last_applied(%lu)",
       current_term_, new_term, commit_index(), last_applied());
  //TODO(anan) BecameCandidate will conflict this assert
  //assert(current_term_ <= new_term);
  if (current_term_ > new_term) {
    return;
  }
  if (current_term_ < new_term) {
    current_term_ = new_term;
    voted_for_ip_ = "";
    voted_for_port_ = 0;
    MetaApply();
  }
  if (!leader_ip.empty() && leader_port != 0) {
    leader_ip_ = leader_ip;
    leader_port_ = leader_port;
  }
  role_ = Role::kFollower;
}

void FloydContext::BecomeCandidate() {
  assert(role_ == Role::kFollower || role_ == Role::kCandidate);
  slash::RWLock l(&stat_rw_, true);
  switch(role_) {
  case Role::kFollower:
    LOGV(INFO_LEVEL, info_log_, "Become Candidate since prev leader timeout, prev term: %lu, prev leader is (%s:%d)",
         current_term_, leader_ip_.c_str(), leader_port_);
    break; 
  case Role::kCandidate:
    LOGV(INFO_LEVEL, info_log_, "Become Candidate since prev election timeout, prev term: %lu",
         current_term_, leader_ip_.c_str(), leader_port_);
    break; 
  default:
    LOGV(INFO_LEVEL, info_log_, "Become Candidate, should not be here, role: %d", role_);
  }

  current_term_++;
  role_ = Role::kCandidate;
  leader_ip_.clear();
  leader_port_ = 0;
  voted_for_ip_ = options_.local_ip;
  voted_for_port_ = options_.local_port;
  vote_quorum_ = 1;
  MetaApply();
}

void FloydContext::BecomeLeader() {
  slash::RWLock l(&stat_rw_, true);
  if (role_ == Role::kLeader) {
    LOGV(INFO_LEVEL, info_log_, "FloydContext::BecomeLeader already Leader!!");
    return;
  }
  role_ = Role::kLeader;
  leader_ip_ = options_.local_ip;
  leader_port_ = options_.local_port;
  LOGV(INFO_LEVEL, info_log_, "FloydContext::BecomeLeader I am become Leader!!");
}

bool FloydContext::AdvanceCommitIndex(uint64_t new_commit_index) {
  if (new_commit_index == 0) {
    return false;
  }
  slash::MutexLock l(&commit_mu_);
  LOGV(DEBUG_LEVEL, info_log_, "FloydContext::AdvanceCommitIndex commit_index=%lu, new commit_index=%lu",
       commit_index_, new_commit_index);
  if (commit_index_ >= new_commit_index) {
    return false;
  }

  uint64_t last_log_index = raft_log_->GetLastLogIndex();
  new_commit_index = std::min(last_log_index, new_commit_index);
  Entry entry;
  raft_log_->GetEntry(new_commit_index, &entry);
  if (entry.term() == current_term_) {
    commit_index_ = new_commit_index;
    LOGV(DEBUG_LEVEL, info_log_, "FloydContext::AdvanceCommitIndex advance commit_index to %ld", new_commit_index);
    return true;
  }
  return false;
}

uint64_t FloydContext::NextApplyIndex(uint64_t* len) {
  //slash::MutexLock lcommit(&commit_mu_);
  uint64_t tcommit_index = commit_index();
  *len = 0;
  slash::MutexLock lapply(&apply_mu_);
  if (tcommit_index > last_applied_) {
    *len = tcommit_index - last_applied_;
  }
  return last_applied_ + 1;
}

void FloydContext::ApplyDone(uint64_t index) {
  slash::MutexLock lapply(&apply_mu_);
  last_applied_ = index; 
  apply_cond_.SignalAll();
}

void FloydContext::MetaApply() {
  raft_log_->UpdateMetadata(current_term_, voted_for_ip_, voted_for_port_, last_applied());
}

bool FloydContext::VoteAndCheck(uint64_t vote_term) {
  slash::RWLock l(&stat_rw_, true);
  LOGV(DEBUG_LEVEL, info_log_, "FloydContext::VoteAndCheck: current_term=%lu vote_term=%lu vote_quorum_=%d",
       current_term_, vote_term, vote_quorum_);
  if (current_term_ != vote_term) {
    return false;
  }
  return (++vote_quorum_) > (options_.members.size() / 2);
}

Status FloydContext::WaitApply(uint64_t last_applied, uint32_t timeout) { 
  slash::MutexLock lapply(&apply_mu_);
  while (last_applied_ < last_applied) {
    if (!apply_cond_.TimedWait(timeout)) {
      return Status::Timeout("apply timeout");
    }
  }
  return Status::OK();
}

// Peer ask my vote with it's ip, port, log_term and log_index
bool FloydContext::ReceiverDoRequestVote(uint64_t term, const std::string ip,
                               int port, uint64_t log_term, uint64_t log_index,
                               uint64_t *my_term) {
  slash::RWLock l(&stat_rw_, true);
  if (term < current_term_) {
    return false; // stale term
  }

  uint64_t my_log_index;
  uint64_t my_log_term;
  raft_log_->GetLastLogTermAndIndex(&my_log_term, &my_log_index);
  LOGV(DEBUG_LEVEL, info_log_, "FloydContext::RequestVote: my last_log is %lu:%lu, sponsor is %lu:%lu",
       my_log_term, my_log_index, log_term, log_index);
  if (log_term < my_log_term
      || (log_term == my_log_term && log_index < my_log_index)) {
    LOGV(INFO_LEVEL, info_log_, "FloydContext::RequestVote: log index not up-to-date, my is %lu:%lu, sponsor is %lu:%lu",
         my_log_term, my_log_index, log_term, log_index);
    return false; // log index is not up-to-date as mine
  }

  if (!voted_for_ip_.empty()
      && (voted_for_ip_ != ip || voted_for_port_ != port)) {
    LOGV(INFO_LEVEL, info_log_, "FloydContext::RequestVote: I have vote for (%s:%d) already.",
         voted_for_ip_.c_str(), voted_for_port_);
    return false; // I have vote someone else
  }

  // Got my vote
  voted_for_ip_ = ip;
  voted_for_port_ = port;
  *my_term = current_term_;
  MetaApply();
  LOGV(INFO_LEVEL, info_log_, "FloydContext::RequestVote: grant vote for (%s:%d),"
       " with my_term(%lu), my last_log(%lu:%lu), sponsor log(%lu,%lu).",
       voted_for_ip_.c_str(), voted_for_port_, *my_term,
       my_log_term, my_log_index, log_term, log_index);
  return true;
}

bool FloydContext::ReceiverDoAppendEntries(uint64_t term,
                                 uint64_t pre_log_term, uint64_t pre_log_index,
                                 std::vector<Entry*>& entries, uint64_t* my_term) {
  slash::RWLock l(&stat_rw_, true);
  // Check pre_log match local log entry
  uint64_t last_log_index = raft_log_->GetLastLogIndex();
  if (pre_log_index > last_log_index) {
    LOGV(DEBUG_LEVEL, info_log_, "FloydContext::ReceiverDoAppendEntries: pre_log(%lu, %lu) > last_log_index(%lu)",
         pre_log_term, pre_log_index, last_log_index);
    return false;
  }

  uint64_t my_log_term = 0;
  Entry entry;
  LOGV(DEBUG_LEVEL, info_log_, "FloydContext::ReceiverDoAppendEntries %llu pre_log_index: \n", pre_log_index);
  if (raft_log_->GetEntry(pre_log_index, &entry) == 0) {
    my_log_term = entry.term();
  } else {
    LOGV(WARN_LEVEL, info_log_, "FloydContext::ReceiverDoAppendEntries: can't get Entry from raft_log pre_log_index %llu", pre_log_index);
  }
  if (pre_log_term != my_log_term) {
    LOGV(WARN_LEVEL, info_log_, "FloydContext::AppendEntries: pre_log(%lu, %lu) don't match with"
         " local log(%lu, %lu), truncate suffix from here",
         pre_log_term, pre_log_index, my_log_term, last_log_index);
    // TruncateSuffix [pre_log_index, last_log_index)
    // raft_log_->TruncateSuffix(pre_log_index - 1);
    return false;
  }

  // Append entry
  if (pre_log_index < last_log_index) {
#ifndef NDEBUG
    uint64_t last_log_term;
    raft_log_->GetLastLogTermAndIndex(&last_log_term, &last_log_index);
    LOGV(DEBUG_LEVEL, info_log_, "FloydContext::AppendEntries: truncate suffix from %lu, "
         "pre_log(%lu,%lu), last_log(%lu,%lu)",
         pre_log_index + 1, pre_log_term, pre_log_index, last_log_term, last_log_index);
#endif
    // TruncateSuffix [pre_log_index + 1, last_log_index)
    // raft_log_->TruncateSuffix(pre_log_index);
  }
  *my_term = current_term_;
  if (entries.size() > 0) {
    LOGV(DEBUG_LEVEL, info_log_, "FloydContext::AppendEntries will append %u entries from "
         " pre_log_index %lu", entries.size(), pre_log_index + 1);
    if (raft_log_->Append(entries) <= 0) {
      return false;
    }
  }
  return true;
}

}  // namespace floyd
