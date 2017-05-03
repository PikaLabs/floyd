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

//struct Argument {
//  FloydPrimary* ptr;
//  int64_t term;
//
//  Argument(FloydPrimary* _ptr, int64_t _term)
//    : ptr(_ptr), term(_term) { }
//};

FloydPrimary::FloydPrimary(FloydContext* context, FloydApply* apply)
  : context_(context),
    apply_(apply),
//    log_(log),
    elect_leader_reset_(false) {
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
    case kCheckElectLeader: {
      uint64_t timeout = context_->GetElectLeaderTimeout();
      LOG_INFO("FloydPrimary::AddTask will CheckElectLeader in %dms", timeout);
      bg_thread_.DelaySchedule(timeout, DoCheckElectLeader, this);
      break;
    }
    case kBecomeLeader: {
      //TODO(anan) cancel ElectLeader task
      LOG_INFO("FloydPrimary::AddTask BecomeLeader");
      bg_thread_.Schedule(DoBecomeLeader, this);
      break;
    }
    //case kBecomeFollower: {
    //  int64_t new_term = *(int64_t)arg;
    //  bg_thread_.Schedule(DoBecomeFollower, new Argument(this, new_term));
    //  break;
    //}
    default: {
      LOG_INFO("FloydPrimary:: unknown task type %d", type);
    }
  }
}


void FloydPrimary::DoCheckElectLeader(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  LOG_DEBUG("FloydPrimary::DoCheckElectLeader");
  ptr->CheckElectLeader();
}

void FloydPrimary::CheckElectLeader() {
  if (context_->role() == Role::kLeader) {
    LOG_DEBUG("FloydPrimary::CheckElectLeader already Leader, Stop check");
    AddTask(kCheckElectLeader);
    return;
  }
  if (context_->role() == Role::kFollower && elect_leader_reset_) {
    LOG_DEBUG("FloydPrimary::CheckElectLeader still live");
    elect_leader_reset_ = false;
  } else {
    LOG_DEBUG("FloydPrimary::CheckElectLeader start Elect leader after timeout");
    context_->BecomeCandidate();
    for (auto& peer : *peers_) {
      peer.second->AddRequestVoteTask();
    }
  }
  AddTask(kCheckElectLeader);
}

void FloydPrimary::DoBecomeLeader(void *arg) {
  FloydPrimary* ptr = static_cast<FloydPrimary*>(arg);
  LOG_DEBUG("FloydPrimary::DoBecomeLeader");
  ptr->BecomeLeader();
}

void FloydPrimary::BecomeLeader() {
  if (context_->role() == Role::kLeader) {
    LOG_DEBUG("FloydPrimary::BecomeLeader already Leader");
    return;
  }
  LOG_DEBUG("FloydPrimary::BecomeLeader");
  context_->BecomeLeader();
  for (auto& peer : *peers_) {
    peer.second->BecomeLeader();
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

void FloydPrimary::AdvanceCommitIndex() {
  if (context_->role() != Role::kLeader) {
    return;
  }

  uint64_t new_commit_index = QuorumMatchIndex();
  LOG_DEBUG("FloydPrimary::AdvanceCommitIndex new_commit_index=%lu", new_commit_index);
  if (context_->AdvanceCommitIndex(new_commit_index)) {
    LOG_DEBUG("FloydPrimary::AdvanceCommitIndex ok, ScheduleApply");
    apply_->ScheduleApply();
  }
}

} // namespace floyd
