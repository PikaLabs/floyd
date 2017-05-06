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
    reset_time_(0) {
  srand(time(NULL));
} 
int FloydPrimary::Start() {
  bg_thread_.set_thread_name("FloydPrimary");
  return bg_thread_.StartThread();
}

FloydPrimary::~FloydPrimary() {
  LOG_INFO("FloydPrimary exit!!!");
  //bg_thread_.set_runing(false);
}

void FloydPrimary::SetPeers(PeersSet* peers) {
  LOG_DEBUG("FloydPrimary::SetPeers peers has %d pairs", peers->size());
  peers_ = peers;
}

void FloydPrimary::AddTask(TaskType type, void* arg) {
  switch (type) {
    case kLeaderHeartbeat:
    case kCheckElectLeader: {
      uint64_t timeout = context_->GetElectLeaderTimeout();
      if (reset_time_) {
        uint64_t delta = (slash::NowMicros() - reset_time_);
        timeout = (delta < timeout) ? (timeout - delta) : 0;
        reset_time_ = 0;
      }
      LOG_INFO("FloydPrimary::AddTask will %s in %dms", 
          (type == kHeartbeat ? "kHeartbeat" : kCheckElectLeader), timeout);
      bg_thread_.DelaySchedule(timeout, DoTimingTask, this);
      break;
    }
    case kBecomeLeader: {
      LOG_INFO("FloydPrimary::AddTask BecomeLeader");
      bg_thread_.Schedule(DoBecomeLeader, this);
      break;
    }
    case kNewCommand: {
      LOG_INFO("FloydPrimary::AddTask NewCommand");
      bg_thread_.Schedule(DoNewCommand, this);
      break;
    }
    case kAdvanceCommitIndex: {
      LOG_INFO("FloydPrimary::AddTask AddvanceCommitIndex");
      bg_thread_.Schedule(DoAdvanceCommitIndex, this);
      break;
    }
    default: {
      LOG_INFO("FloydPrimary:: unknown task type %d", type);
    }
  }
}

void FloydPrimary::DoTimingTask(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  if (ptr->reset_time_) {
    return;
  }
  LOG_DEBUG("FloydPrimary::DoCheckElectLeader");
  if (ptr->context_->role() == Role::kLeader) {
    ptr->LeaderHeartbeat();
  } else {
    ptr->CheckElectLeader();
  }
}

void FloydPrimary::LeaderHeartbeat() {
  if (context_->role() == Role::kLeader) {
    LOG_DEBUG("FloydPrimary:: LeaderHeartbeat");
    NoticePeerTask(kLeaderHeartbeat);
  }
  AddTask(kLeaderHeartbeat);
}

void FloydPrimary::CheckElectLeader() {
  if (context_->role() != Role::kLeader) {
    LOG_DEBUG("FloydPrimary::CheckElectLeader start Elect leader after timeout");
    context_->BecomeCandidate();
    NoticePeerTask(kCheckElectLeader);
  }
  AddTask(kCheckElectLeader);
}

void FloydPrimary::DoBecomeLeader(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  LOG_DEBUG("FloydPrimary::DoBecomeLeader");
  if (ptr->context_->role() == Role::kLeader) {
    LOG_WARN("FloydPrimary::BecomeLeader already Leader");
    return;
  }
  LOG_DEBUG("FloydPrimary::BecomeLeader");
  ptr->context_->BecomeLeader();
  ptr->NoticePeerTask(kLeaderHeartbeat);
  ptr->AddTask(kLeaderHeartbeat);
}

void FloydPrimary::DoNewCommand(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  LOG_DEBUG("FloydPrimary::DoNewCommand");
  if (ptr->context_->role() != Role::kLeader) {
    LOG_WARN("FloydPrimary::NewCommand, Not leader yet");
    return;
  }
  ptr->NoticePeerTask(kNewCommand);
  ptr->ResetCronTimer();
}

void FloydPrimary::DoAdvanceCommitIndex(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  if (ptr->context_->role() != Role::kLeader) {
    LOG_WARN("FloydPrimary::kAdvanceCommitIndex not leader");
    return;
  }

  uint64_t new_commit_index = ptr->QuorumMatchIndex();
  LOG_DEBUG("FloydPrimary::AdvanceCommitIndex new_commit_index=%lu", new_commit_index);
  if (ptr->context_->AdvanceCommitIndex(new_commit_index)) {
    LOG_DEBUG("FloydPrimary::AdvanceCommitIndex ok, ScheduleApply");
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
      default:
        LOG_WARN("Error TaskType to notice peer");
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
