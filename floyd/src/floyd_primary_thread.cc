// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_primary_thread.h"

#include <stdlib.h>
#include <time.h>
#include <google/protobuf/text_format.h>

#include <climits>
#include <algorithm>
#include <vector>

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"

#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/raft_meta.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"
#include "floyd/include/floyd_options.h"

namespace floyd {

FloydPrimary::FloydPrimary(FloydContext* context, PeersSet* peers, RaftMeta* raft_meta,
    const Options& options, Logger* info_log)
  : context_(context),
    peers_(peers),
    raft_meta_(raft_meta),
    options_(options),
    info_log_(info_log) {
}

int FloydPrimary::Start() {
  bg_thread_.set_thread_name("R:" + std::to_string(options_.local_port));
  return bg_thread_.StartThread();
}

FloydPrimary::~FloydPrimary() {
  LOGV(INFO_LEVEL, info_log_, "FloydPrimary::~FloydPrimary Primary thread exit");
}

int FloydPrimary::Stop() {
  return bg_thread_.StopThread();
}

void FloydPrimary::AddTask(TaskType type, bool is_delay) {
  /*
   * int timer_queue_size, queue_size;
   * bg_thread_.QueueSize(&timer_queue_size, &queue_size);
   * LOGV(INFO_LEVEL, info_log_, "FloydPrimary::AddTask timer_queue size %d queue_size %d tasktype %d is_delay %d",
   *     timer_queue_size, queue_size, type, is_delay);
   */
  switch (type) {
    case kHeartBeat:
      if (is_delay) {
        uint64_t timeout = options_.heartbeat_us;
        bg_thread_.DelaySchedule(timeout / 1000LL, LaunchHeartBeatWrapper, this);
      } else {
        bg_thread_.Schedule(LaunchHeartBeatWrapper, this);
      }
      break;
    case kCheckLeader:
      if (is_delay) {
        uint64_t timeout = options_.check_leader_us;
        bg_thread_.DelaySchedule(timeout / 1000LL, LaunchCheckLeaderWrapper, this);
      } else {
        bg_thread_.Schedule(LaunchCheckLeaderWrapper, this);
      }
      break;
    case kNewCommand:
      bg_thread_.Schedule(LaunchNewCommandWrapper, this);
      break;
    default:
      LOGV(WARN_LEVEL, info_log_, "FloydPrimary:: unknown task type %d", type);
      break;
  }
}

void FloydPrimary::LaunchHeartBeatWrapper(void *arg) {
  reinterpret_cast<FloydPrimary *>(arg)->LaunchHeartBeat();
}

void FloydPrimary::LaunchHeartBeat() {
  slash::MutexLock l(&context_->global_mu);
  if (context_->role == Role::kLeader) {
    NoticePeerTask(kNewCommand);
    AddTask(kHeartBeat);
  }
}

void FloydPrimary::LaunchCheckLeaderWrapper(void *arg) {
  reinterpret_cast<FloydPrimary *>(arg)->LaunchCheckLeader();
}

void FloydPrimary::LaunchCheckLeader() {
  slash::MutexLock l(&context_->global_mu);
  if (context_->role == Role::kFollower || context_->role == Role::kCandidate) {
    if (options_.single_mode) {
      context_->BecomeLeader();
      context_->voted_for_ip = options_.local_ip;
      context_->voted_for_port = options_.local_port;
      raft_meta_->SetCurrentTerm(context_->current_term);
      raft_meta_->SetVotedForIp(context_->voted_for_ip);
      raft_meta_->SetVotedForPort(context_->voted_for_port);
    } else if (context_->last_op_time + options_.check_leader_us < slash::NowMicros()) {
      context_->BecomeCandidate();
      LOGV(INFO_LEVEL, info_log_, "FloydPrimary::LaunchCheckLeader: %s:%d Become Candidate because of timeout, new term is %d"
         " voted for %s:%d", options_.local_ip.c_str(), options_.local_port, context_->current_term,
         context_->voted_for_ip.c_str(), context_->voted_for_port);
      raft_meta_->SetCurrentTerm(context_->current_term);
      raft_meta_->SetVotedForIp(context_->voted_for_ip);
      raft_meta_->SetVotedForPort(context_->voted_for_port);
      NoticePeerTask(kHeartBeat);
    }
  }
  AddTask(kCheckLeader);
}

void FloydPrimary::LaunchNewCommandWrapper(void *arg) {
  reinterpret_cast<FloydPrimary *>(arg)->LaunchNewCommand();
}

void FloydPrimary::LaunchNewCommand() {
  LOGV(DEBUG_LEVEL, info_log_, "FloydPrimary::LaunchNewCommand");
  if (context_->role != Role::kLeader) {
    LOGV(WARN_LEVEL, info_log_, "FloydPrimary::LaunchNewCommand, Not leader yet");
    return;
  }
  NoticePeerTask(kNewCommand);
}

// when adding task to peer thread, we can consider that this job have been in the network
// even it is still in the peer thread's queue
void FloydPrimary::NoticePeerTask(TaskType type) {
  for (auto& peer : (*peers_)) {
    switch (type) {
    case kHeartBeat:
      LOGV(INFO_LEVEL, info_log_, "FloydPrimary::NoticePeerTask server %s:%d Add request Task to queue to %s at term %d",
          options_.local_ip.c_str(), options_.local_port, peer.second->peer_addr().c_str(), context_->current_term);
      peer.second->AddRequestVoteTask();
      break;
    case kNewCommand:
      LOGV(DEBUG_LEVEL, info_log_, "FloydPrimary::NoticePeerTask server %s:%d Add appendEntries Task to queue to %s at term %d",
          options_.local_ip.c_str(), options_.local_port, peer.second->peer_addr().c_str(), context_->current_term);
      peer.second->AddAppendEntriesTask();
      break;
    default:
      LOGV(WARN_LEVEL, info_log_, "FloydPrimary::NoticePeerTask server %s:%d Error TaskType to notice peer",
          options_.local_ip.c_str(), options_.local_port);
    }
  }
}

}  // namespace floyd
