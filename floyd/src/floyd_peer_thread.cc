// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_peer_thread.h"

#include <climits>
#include <google/protobuf/text_format.h>
#include "floyd/src/floyd_primary_thread.h"
#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/raft_log.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/xdebug.h"

namespace floyd {

Peer::Peer(std::string server, FloydContext* context, FloydPrimary* primary,
           RaftLog* raft_log, ClientPool* pool)
  : server_(server),
    context_(context),
    primary_(primary),
    raft_log_(raft_log),
    pool_(pool),
    next_index_(1) {
}

int Peer::StartThread() {
  bg_thread_.set_thread_name("FloydPr" + server_.substr(server_.find(':')));
  return bg_thread_.StartThread();
}

Peer::~Peer() {
  LOGV(INFO_LEVEL, context_->info_log(), "Peer(%s) exit!!!", server_.c_str());
}

void Peer::set_next_index(uint64_t next_index) {
  next_index_ = next_index;
}

uint64_t Peer::get_next_index() {
  return next_index_;
}

void Peer::AddRequestVoteTask() {
  bg_thread_.Schedule(RequestVoteRPCWrapper, this);
}

void Peer::RequestVoteRPCWrapper(void *arg) {
  Peer *peer = static_cast<Peer*>(arg);
  LOGV(DEBUG_LEVEL, peer->context_->info_log(), "Peer(%s)::DoRequestVote",
       peer->server_.c_str());
  Status result = peer->RequestVoteRPC();
  if (!result.ok()) {
    LOGV(ERROR_LEVEL, peer->context_->info_log(), "Peer(%s) failed to "
         "RequestVote caz %s.", peer->server_.c_str(), result.ToString().c_str());
  }
}

Status Peer::RequestVoteRPC() {
  /*
   * 这里为什么需要判断一下是否是 candidate, 如果保证调用requestvote 之前就肯定是candidate
   * 就不需要这个保证了吧
   */
  if (context_->role() != Role::kCandidate) {
    LOGV(DEBUG_LEVEL, context_->info_log(), "Peer(%s) not candidate,"
         "skip RequestVote", server_.c_str());
    return Status::OK();
  }

  // TODO (anan) log->getEntry() need lock
  uint64_t last_log_term;
  uint64_t last_log_index;
  context_->raft_log()->GetLastLogTermAndIndex(&last_log_term, &last_log_index);
  uint64_t current_term = context_->current_term();

  CmdRequest req;
  req.set_type(Type::RequestVote);
  CmdRequest_RequestVote* request_vote = req.mutable_request_vote();
  request_vote->set_ip(context_->local_ip());
  request_vote->set_port(context_->local_port());
  request_vote->set_term(current_term);
  request_vote->set_last_log_term(last_log_term);
  request_vote->set_last_log_index(last_log_index);

#ifndef NDEBUG
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(req, &text_format);
  LOGV(DEBUG_LEVEL, context_->info_log(), "Send RequestVote to %s, message :"
       "\n%s", server_.c_str(), text_format.c_str());
#endif

  CmdResponse res;
  Status result = pool_->SendAndRecv(server_, req, &res);

  if (!result.ok()) {
    LOGV(DEBUG_LEVEL, context_->info_log(), "RequestVote to %s failed %s",
         server_.c_str(), result.ToString().c_str());
    return result;
  }

#ifndef NDEBUG 
  google::protobuf::TextFormat::PrintToString(res, &text_format);
  LOGV(DEBUG_LEVEL, context_->info_log(), "Recv RequestVote from %s, message :"
       "\n%s", server_.c_str(), text_format.c_str());
#endif

  // we get term from request vote
  uint64_t res_term = res.request_vote().term();
  if (result.ok() && context_->role() == Role::kCandidate) {
    // kOk means RequestVote success, opposite vote for me
    if (res.code() == StatusCode::kOk) {    // granted
      LOGV(INFO_LEVEL, context_->info_log(), "Peer(%s)::RequestVote granted"
           " will Vote and check", server_.c_str());
      // However, we need check whether this vote is vote for old term
      // we need igore these type of vote
      if (context_->VoteAndCheck(res_term)) {
        primary_->AddTask(kBecomeLeader);  
      }
    } else {
      // opposite RequestVote fail, maybe opposite has larger term, or opposite has
      // longer log.
      // if opposite has larger term, this node will become follower
      // otherwise we will do nothing
      LOGV(DEBUG_LEVEL, context_->info_log(), "Vote request denied by %s,"
           " res_term=%lu, current_term=%lu",
           server_.c_str(), res_term, current_term);
      if (res_term > current_term) {
        //TODO(anan) maybe combine these 2 steps
        context_->BecomeFollower(res_term);
        primary_->ResetElectLeaderTimer();
      }
    }
  }

  return result;
}

void Peer::AddHeartBeatTask() {
  AddAppendEntriesTask();
}

void Peer::AddBecomeLeaderTask() {
  next_index_ = raft_log_->GetLastLogIndex() + 1;
  AddAppendEntriesTask();
}

void Peer::AddAppendEntriesTask() {
  bg_thread_.Schedule(AppendEntriesRPCWrapper, this);
}

uint64_t Peer::GetMatchIndex() {
  return (next_index_ - 1);
}

void Peer::AppendEntriesRPCWrapper(void *arg) {
  Peer* peer = static_cast<Peer*>(arg);
  LOGV(DEBUG_LEVEL, peer->context_->info_log(), "Peer(%s) DoAppendEntries",
       peer->server_.c_str());
  Status result = peer->AppendEntriesRPC();
  if (!result.ok()) {
    LOGV(ERROR_LEVEL, peer->context_->info_log(), "Peer(%s) failed to "
         "AppendEntries caz %s.",
         peer->server_.c_str(), result.ToString().c_str());
  }
}

Status Peer::AppendEntriesRPC() {
  if (context_->role() != Role::kLeader) {
    LOGV(WARN_LEVEL, context_->info_log(), "Peer(%s) not leader anymore,"
         "skip AppendEntries", server_.c_str());
    return Status::OK();
  }

  uint64_t last_log_index = raft_log_->GetLastLogIndex();
  uint64_t prev_log_index = next_index_ - 1;
  if (prev_log_index > last_log_index) {
    return Status::InvalidArgument("prev_Log_index > last_log_index");
  }

  uint64_t prev_log_term = 0;
  if (prev_log_index != 0) {
    Entry entry;
    if (raft_log_->GetEntry(prev_log_index, &entry) != 0) {
      LOGV(WARN_LEVEL, context_->info_log(), "Peer::AppendEntriesRPC:GetEntry index %llu "
          "not found", prev_log_index);
    } else {
      prev_log_term = entry.term();
    }
  }

  CmdRequest req;
  CmdRequest_AppendEntries* append_entries = req.mutable_append_entries();
  req.set_type(Type::AppendEntries);
  append_entries->set_ip(context_->local_ip());
  append_entries->set_port(context_->local_port());
  append_entries->set_term(context_->current_term());
  append_entries->set_prev_log_index(prev_log_index);
  append_entries->set_prev_log_term(prev_log_term);
  if (prev_log_index == 0) {
    LOGV(WARN_LEVEL, context_->info_log(), "FloydPeerThread::AppendEntries: prev_log_index is 0");
  }

  uint64_t num_entries = 0;
  Entry *tmp_entry = new Entry();
  LOGV(DEBUG_LEVEL, context_->info_log(), "next_index_ %llu, last_log_index %llu", next_index_.load(), last_log_index);
  for (uint64_t index = next_index_; index <= last_log_index; index++) {
    if (raft_log_->GetEntry(index, tmp_entry) == 0) {
      // TODO(baotiao) how to avoid memory copy here
      Entry *entry = append_entries->add_entries();
      *entry = *tmp_entry;
    } else {
      LOGV(WARN_LEVEL, context_->info_log(), "FloydPeerThread::AppendEntries: can't get Entry from raft_log, index %lld", index);
      break;
    }

    num_entries++;
    if (num_entries >= context_->append_entries_count_once() 
        || (uint64_t)append_entries->ByteSize() >= context_->append_entries_size_once()) {
      break;
    }
  }
  delete tmp_entry;
  append_entries->set_commit_index(
      std::min(context_->commit_index(), prev_log_index + num_entries));

  CmdResponse res;
  Status result = pool_->SendAndRecv(server_, req, &res);

  if (!result.ok()) {
    LOGV(DEBUG_LEVEL, context_->info_log(), "AppendEntry to %s failed %s",
         server_.c_str(), result.ToString().c_str());
    return result;
  }
  uint64_t res_term = res.append_entries().term();
  if (result.ok() && res_term > context_->current_term()) {
    // TODO(anan) maybe combine these 2 steps
    context_->BecomeFollower(res_term);
    primary_->ResetElectLeaderTimer();
  }

  if (result.ok() && context_->role() == Role::kLeader) {
    if (res.code() == StatusCode::kOk) {
      next_index_ = prev_log_index + num_entries + 1;
      LOGV(DEBUG_LEVEL, context_->info_log(), "next_index_ %lld prev_log_index \
          %lld num_entries %lld", next_index_.load(), prev_log_index, num_entries);
      primary_->AddTask(kAdvanceCommitIndex);

      // If this follower is far behind leader, and there is no more
      // AppendEntryTask, we should add one
      if (next_index_ + context_->append_entries_count_once() < last_log_index) {
        int pri_size, qu_size;
        bg_thread_.QueueSize(&pri_size, &qu_size);
        if (qu_size < 1) {
          LOGV(DEBUG_LEVEL, context_->info_log(), "AppendEntry again "
               "to catch up next_index(%llu) last_log_index(%llu)",
               next_index_.load(), last_log_index);
          AddAppendEntriesTask();
        }
      }
    } else {
      uint64_t adjust_index = std::min(res.append_entries().last_log_index() + 1,
                                       next_index_ - 1);
      if (adjust_index > 0) {
        // Prev log don't match, so we retry with more prev one according to
        // response
        next_index_ = adjust_index;
        LOGV(DEBUG_LEVEL, context_->info_log(), "update next_index_ %lld", next_index_.load());
        AddAppendEntriesTask();
      }
    }
  }
  return result;
}

} // namespace floyd
