// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_peer_thread.h"

#include <climits>
#include <google/protobuf/text_format.h>

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/xdebug.h"

#include "floyd/src/floyd_primary_thread.h"
#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/raft_log.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"
#include "floyd/src/raft_meta.h"
#include "floyd/src/floyd_apply.h"

namespace floyd {

Peer::Peer(std::string server, FloydContext* context, FloydPrimary* primary, RaftMeta* raft_meta,
    RaftLog* raft_log, ClientPool* pool, FloydApply* apply, const Options& options, Logger* info_log)
  : server_(server),
    context_(context),
    primary_(primary),
    raft_meta_(raft_meta),
    raft_log_(raft_log),
    pool_(pool),
    apply_(apply),
    options_(options),
    info_log_(info_log),
    next_index_(1),
    match_index_(0) {
      next_index_ = raft_log_->GetLastLogIndex() + 1;
      match_index_ = raft_meta_->GetLastApplied();
}

int Peer::Start() {
  bg_thread_.set_thread_name("FloydPeer" + server_.substr(server_.find(':')));
  return bg_thread_.StartThread();
}

Peer::~Peer() {
  LOGV(INFO_LEVEL, info_log_, "Peer(%s) exit!!!", server_.c_str());
}
int Peer::Stop() {
  return bg_thread_.StopThread();
}


bool Peer::CheckAndVote(uint64_t vote_term) {
  if (context_->current_term != vote_term) {
    return false;
  }
  return (++context_->vote_quorum) > (options_.members.size() / 2);
}

void Peer::AddRequestVoteTask() {
  bg_thread_.Schedule(&RequestVoteRPCWrapper, this);
}

void Peer::RequestVoteRPCWrapper(void *arg) {
  reinterpret_cast<Peer*>(arg)->RequestVoteRPC();
}

Status Peer::RequestVoteRPC() {
  uint64_t last_log_term;
  uint64_t last_log_index;
  CmdRequest req;
  {
  slash::MutexLock l(&context_->global_mu);
  raft_log_->GetLastLogTermAndIndex(&last_log_term, &last_log_index);

  req.set_type(Type::kRequestVote);
  CmdRequest_RequestVote* request_vote = req.mutable_request_vote();
  request_vote->set_ip(options_.local_ip);
  request_vote->set_port(options_.local_port);
  request_vote->set_term(context_->current_term);
  request_vote->set_last_log_term(last_log_term);
  request_vote->set_last_log_index(last_log_index);
  }

  CmdResponse res;
  Status result = pool_->SendAndRecv(server_, req, &res);

  if (!result.ok()) {
    LOGV(DEBUG_LEVEL, info_log_, "RequestVote to %s failed %s",
         server_.c_str(), result.ToString().c_str());
    return result;
  }

  {
  slash::MutexLock l(&context_->global_mu);
  if (context_->role == Role::kCandidate) {
    // kOk means RequestVote success, opposite vote for me
    if (res.request_vote_res().vote_granted() == true) {    // granted
      LOGV(INFO_LEVEL, info_log_, "Peer(%s)::RequestVote granted will Vote and check", server_.c_str());
      // However, we need check whether this vote is vote for old term
      // we need igore these type of vote
      if (CheckAndVote(res.request_vote_res().term())) {
        context_->BecomeLeader();
        primary_->AddTask(kHeartBeat, false);
      }
    } else {
      if (res.request_vote_res().term() > context_->current_term) {
        context_->BecomeFollower(res.request_vote_res().term());
        raft_meta_->SetCurrentTerm(context_->current_term);
        raft_meta_->SetVotedForIp(context_->voted_for_ip);
        raft_meta_->SetVotedForPort(context_->voted_for_port);
      }
      // opposite RequestVote fail, maybe opposite has larger term, or opposite has
      // longer log. if opposite has larger term, this node will become follower
      // otherwise we will do nothing
      LOGV(DEBUG_LEVEL, info_log_, "Vote request denied by %s,"
          " res_term=%lu, current_term=%lu",
          server_.c_str(), res.request_vote_res().term(), context_->current_term);
    }
  } else {
    // TODO(ba0tiao) if i am not longer candidate
  }
  }
  return result;
}

void Peer::AddAppendEntriesTask() {
  bg_thread_.Schedule(&AppendEntriesRPCWrapper, this);
}

uint64_t Peer::QuorumMatchIndex() {
  std::vector<uint64_t> values;
  std::map<std::string, Peer*>::iterator iter; 
  for (iter = peers_.begin(); iter != peers_.end(); iter++) {
    if (iter->first == server_) {
      continue;
    }
    values.push_back(iter->second->match_index());
  }
  std::sort(values.begin(), values.end());
  return values.at(values.size() / 2);
}

// only leader will call AdvanceCommitIndex
// follower only need set commit as leader's
void Peer::AdvanceLeaderCommitIndex() {
  Entry entry;
  uint64_t new_commit_index = QuorumMatchIndex();
  if (context_->commit_index != new_commit_index) {
    context_->commit_index = new_commit_index;
    raft_meta_->SetCommitIndex(context_->commit_index);
  }
  return;
}

void Peer::AppendEntriesRPCWrapper(void *arg) {
  reinterpret_cast<Peer*>(arg)->AppendEntriesRPC();
}

Status Peer::AppendEntriesRPC() {
  uint64_t prev_log_index = next_index_ - 1;
  uint64_t num_entries = 0;
  uint64_t prev_log_term = 0;
  uint64_t last_log_index = 0;
  CmdRequest req;
  CmdRequest_AppendEntries* append_entries = req.mutable_append_entries();
  {
  slash::MutexLock l(&context_->global_mu);

  if (prev_log_index != 0) {
    Entry entry;
    if (raft_log_->GetEntry(prev_log_index, &entry) != 0) {
      LOGV(WARN_LEVEL, info_log_, "Peer::AppendEntriesRPC:GetEntry index %llu "
          "not found", prev_log_index);
    } else {
      prev_log_term = entry.term();
    }
  }

  req.set_type(Type::kAppendEntries);
  append_entries->set_ip(options_.local_ip);
  append_entries->set_port(options_.local_port);
  append_entries->set_term(context_->current_term);
  append_entries->set_prev_log_index(prev_log_index);
  append_entries->set_prev_log_term(prev_log_term);

  last_log_index = raft_log_->GetLastLogIndex();
  Entry *tmp_entry = new Entry();
  LOGV(DEBUG_LEVEL, info_log_, "next_index_ %llu, last_log_index %llu", next_index_.load(), last_log_index);
  for (uint64_t index = next_index_; index <= last_log_index; index++) {
    if (raft_log_->GetEntry(index, tmp_entry) == 0) {
      // TODO(ba0tiao) how to avoid memory copy here
      Entry *entry = append_entries->add_entries();
      *entry = *tmp_entry;
    } else {
      LOGV(WARN_LEVEL, info_log_, "FloydPeerThread::AppendEntries: can't get Entry from raft_log, index %lld", index);
      break;
    }

    num_entries++;
    if (num_entries >= options_.append_entries_count_once
        || (uint64_t)append_entries->ByteSize() >= options_.append_entries_size_once) {
      break;
    }
  }
  delete tmp_entry;
  /*
   * commit_index should be min of follower log and leader's commit_index
   * if follower's commit index larger than follower log, it conflict 
   */
  append_entries->set_leader_commit(
      std::min(context_->commit_index, prev_log_index + num_entries));
  }

  CmdResponse res;
  Status result = pool_->SendAndRecv(server_, req, &res);

  {
  slash::MutexLock l(&context_->global_mu);
  if (!result.ok()) {
    LOGV(WARN_LEVEL, info_log_, "FloydPeerThread::AppendEntries: AppendEntry to %s failed %s",
         server_.c_str(), result.ToString().c_str());
    return result;
  }

  // here we may get a larger term, and transfer to follower
  // so we need to judge the role here
  if (context_->role == Role::kLeader) {
    /*
     * receiver has higer term than myself, so turn from candidate to follower
     */
    if (res.append_entries_res().term() > context_->current_term) {
      context_->BecomeFollower(res.append_entries_res().term());
      raft_meta_->SetCurrentTerm(context_->current_term);
      raft_meta_->SetVotedForIp(context_->voted_for_ip);
      raft_meta_->SetVotedForPort(context_->voted_for_port);
    }
    if (res.append_entries_res().success() == true) {
      if (num_entries > 0) {
        match_index_ = prev_log_index + num_entries;
        // only log entries from the leader's current term are committed
        // by counting replicas
        if (append_entries->entries(num_entries - 1).term() == context_->current_term) {
          AdvanceLeaderCommitIndex();
          apply_->ScheduleApply();
        }
        next_index_ = prev_log_index + num_entries + 1;
        // If this follower is far behind leader, and there is no more
        // AppendEntryTask, we should add one
        if (next_index_ + options_.append_entries_count_once < last_log_index) {
          int pri_size, qu_size;
          bg_thread_.QueueSize(&pri_size, &qu_size);
          if (qu_size < 1) {
            LOGV(DEBUG_LEVEL, info_log_, "AppendEntry again "
                "to catch up next_index(%llu) last_log_index(%llu)",
                next_index_.load(), last_log_index);
            AddAppendEntriesTask();
          }
        }
      }
    } else {
      uint64_t adjust_index = std::min(res.append_entries_res().last_log_index() + 1,
                                       next_index_ - 1);
      if (adjust_index > 0) {
        // Prev log don't match, so we retry with more prev one according to
        // response
        next_index_ = adjust_index;
        LOGV(INFO_LEVEL, info_log_, "update next_index_ %lld", next_index_.load());
        AddAppendEntriesTask();
      }
    }
  } else if (context_->role == Role::kFollower) {
    // TODO(ba0tiao) if I am not longer a leader
  } else if (context_->role == Role::kCandidate) {
  }
  }
  return result;
}

} // namespace floyd
