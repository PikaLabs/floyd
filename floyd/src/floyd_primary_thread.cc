#include "floyd/src/floyd_primary_thread.h"

#include <climits>
#include <stdlib.h>
#include <time.h>
#include <google/protobuf/text_format.h>
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/file_log.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

FloydPrimary::FloydPrimary(FloydContext* context, FloydApply* apply)
  : context_(context),
    apply_(apply),
    reset_elect_leader_time_(0),
    reset_leader_heartbeat_time_(0) {
   srand(time(NULL));
} 
int FloydPrimary::Start() {
  bg_thread_.set_thread_name("FloydPrimary");
  return bg_thread_.StartThread();
}

FloydPrimary::~FloydPrimary() {
  LOGV(INFO_LEVEL, context_->info_log(), "FloydPrimary exit!!!");
  //bg_thread_.set_runing(false);
}

void FloydPrimary::SetPeers(PeersSet* peers) {
  LOGV(DEBUG_LEVEL, context_->info_log(), "FloydPrimary::SetPeers peers "
       "has %d pairs", peers->size());
  peers_ = peers;
}

// TODO(anan) We keep 2 Primary Cron in total.
//    1. one short live Cron for LeaderHeartbeat, which is available as a leader;
//    2. another long live Cron for ElectLeaderCheck, which is started when
//    creating Primary;
void FloydPrimary::AddTask(TaskType type, void* arg) {
  switch (type) {
    case kLeaderHeartbeat: {
      uint64_t timeout = context_->heartbeat_us();
      if (reset_leader_heartbeat_time_) {
        uint64_t delta = (slash::NowMicros() - reset_leader_heartbeat_time_);
        LOGV(DEBUG_LEVEL, context_->info_log(), "FloydPrimary::AddTask "
             "kLeaderHeartbeat reset_leader_heartbeat_timer old timeout"
             " %luus, delta is %luus", timeout, delta);
        timeout = (delta < timeout) ? (timeout - delta) : 0;
        reset_leader_heartbeat_time_ = 0;
      }
      LOGV(DEBUG_LEVEL, context_->info_log(), "FloydPrimary::AddTask "
           "kLeaderHeartbeat will in %dms", timeout / 1000LL);
      bg_thread_.DelaySchedule(timeout / 1000LL, DoLeaderHeartbeat, this);
      break;
    }
    case kCheckElectLeader: {
      uint64_t timeout = context_->GetElectLeaderTimeout() * 1000LL;
      if (reset_elect_leader_time_) {
        uint64_t delta = (slash::NowMicros() - reset_elect_leader_time_);
        LOGV(DEBUG_LEVEL, context_->info_log(), "FloydPrimary::AddTask "
             "kCheckElectLeader reset_elect_leader_timer old timeout"
             " %luus, delta is %luus", timeout, delta);
        timeout = (delta < timeout) ? (timeout - delta) : 0;
        reset_elect_leader_time_ = 0;
      }
      LOGV(DEBUG_LEVEL, context_->info_log(), "FloydPrimary::AddTask "
           "kCheckElectLeader will in %dms", timeout / 1000LL);
      bg_thread_.DelaySchedule(timeout / 1000LL, DoCheckElectLeader, this);
      break;
    }
    case kBecomeLeader: {
      LOGV(DEBUG_LEVEL, context_->info_log(), "FloydPrimary::AddTask BecomeLeader");
      bg_thread_.Schedule(DoBecomeLeader, this);
      break;
    }
    case kNewCommand: {
      LOGV(DEBUG_LEVEL, context_->info_log(), "FloydPrimary::AddTask NewCommand");
      bg_thread_.Schedule(DoNewCommand, this);
      break;
    }
    case kAdvanceCommitIndex: {
      LOGV(DEBUG_LEVEL, context_->info_log(), "FloydPrimary::AddTask AddvanceCommitIndex");
      bg_thread_.Schedule(DoAdvanceCommitIndex, this);
      break;
    }
    default: {
      LOGV(WARN_LEVEL, context_->info_log(), "FloydPrimary:: unknown task type %d", type);
    }
  }

#ifndef NDEBUG
  //int pri_size, size;
  //bg_thread_.QueueSize(&pri_size, &size);
  //LOGV(INFO_LEVEL, context_->info_log(), "FloydPrimary Pri queue size %d, "
  //     "normal queue size is %d", pri_size, size);
#endif
}

void FloydPrimary::DoLeaderHeartbeat(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  if (ptr->context_->role() == Role::kLeader) {
    if (ptr->reset_leader_heartbeat_time_ == 0) {
      LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::DoTimingTask"
           " Start LeaderHeartbeat");
      //ptr->LeaderHeartbeat();
      ptr->NoticePeerTask(kLeaderHeartbeat);
    }
    ptr->AddTask(kLeaderHeartbeat);
  }
}

void FloydPrimary::DoCheckElectLeader(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);

  if (ptr->context_->role() == Role::kLeader) {
    LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::DoCheckElectLeader"
         " already Leader skip check");
    ptr->AddTask(kCheckElectLeader);
    return;
  }

  if (ptr->context_->role() == Role::kFollower && ptr->reset_elect_leader_time_) {
    LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::DoCheckElectLeader"
        " still live");
  } else {
    LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::DoCheckElectLeader"
        " start Elect leader after timeout");
    ptr->context_->BecomeCandidate();
    ptr->NoticePeerTask(kCheckElectLeader);
  }
  ptr->AddTask(kCheckElectLeader);
}

void FloydPrimary::DoBecomeLeader(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::DoBecomeLeader");
  if (ptr->context_->role() == Role::kLeader) {
    LOGV(WARN_LEVEL, ptr->context_->info_log(), "FloydPrimary::BecomeLeader already Leader");
    return;
  }
  LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::BecomeLeader");
  ptr->context_->BecomeLeader();
  ptr->NoticePeerTask(kBecomeLeader);
  ptr->AddTask(kLeaderHeartbeat);
}

void FloydPrimary::DoNewCommand(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::DoNewCommand");
  if (ptr->context_->role() != Role::kLeader) {
    LOGV(WARN_LEVEL, ptr->context_->info_log(), "FloydPrimary::NewCommand, Not leader yet");
    return;
  }
  ptr->NoticePeerTask(kNewCommand);
  ptr->ResetLeaderHeartbeatTimer();
}

void FloydPrimary::DoAdvanceCommitIndex(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  if (ptr->context_->role() != Role::kLeader) {
    LOGV(WARN_LEVEL, ptr->context_->info_log(), "FloydPrimary::AdvanceCommitIndex not leader");
    return;
  }

  uint64_t new_commit_index = ptr->QuorumMatchIndex();
  LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::AdvanceCommitIndex"
       " new_commit_index=%lu", new_commit_index);
  if (ptr->context_->AdvanceCommitIndex(new_commit_index)) {
    LOGV(DEBUG_LEVEL, ptr->context_->info_log(), "FloydPrimary::AdvanceCommitIndex ok, ScheduleApply");
    ptr->apply_->ScheduleApply();
  }
}

void FloydPrimary::NoticePeerTask(TaskType type) {
  for (auto& peer : *peers_) {
    switch (type) {
      case kLeaderHeartbeat:
        peer.second->AddHeartBeatTask();
        break;
      case kCheckElectLeader:
        peer.second->AddRequestVoteTask();
        break;
      case kNewCommand:
        peer.second->AddAppendEntriesTask();
        break;
      case kBecomeLeader:
        peer.second->AddBecomeLeaderTask();
        break;
      default:
        LOGV(WARN_LEVEL, context_->info_log(), "Error TaskType to notice peer");
    }
  }
}

uint64_t FloydPrimary::QuorumMatchIndex() {
  std::vector<uint64_t> values;
  for (auto& iter : *peers_) {
    values.push_back(iter.second->GetMatchIndex());
  }
  std::sort(values.begin(), values.end());
  return values.at(values.size() / 2);
}


} // namespace floyd
