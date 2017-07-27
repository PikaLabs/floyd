// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_apply.h"

#include <unistd.h>
#include <string>

#include "slash/include/xdebug.h"
#include <google/protobuf/text_format.h>


#include "floyd/src/logger.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/raft_meta.h"
#include "floyd/src/raft_log.h"


namespace floyd {

FloydApply::FloydApply(FloydContext* context, rocksdb::DB* db, RaftMeta* raft_meta,
    RaftLog* raft_log, Logger* info_log)
  : context_(context),
    db_(db),
    raft_meta_(raft_meta),
    raft_log_(raft_log),
    info_log_(info_log) {
}

FloydApply::~FloydApply() {
}

int FloydApply::Start() {
  bg_thread_.set_thread_name("FloydApply");
  return bg_thread_.StartThread();
}

void FloydApply::ScheduleApply() {
  bg_thread_.Schedule(&ApplyStateMachineWrapper, this);
}

void FloydApply::ApplyStateMachineWrapper(void* arg) {
  reinterpret_cast<FloydApply*>(arg)->ApplyStateMachine();
}

void FloydApply::ApplyStateMachine() {
  uint64_t last_applied = context_->last_applied;
  // Apply as more entry as possible
  uint64_t commit_index;
  context_->commit_index_mu.Lock();
  commit_index = context_->commit_index;
  context_->commit_index_mu.Unlock();

  LOGV(DEBUG_LEVEL, info_log_, "FloydApply::ApplyStateMachine: last_applied: %lu, commit_index: %lu",
            last_applied, commit_index);
  while (last_applied < commit_index) {
    last_applied++;
    Entry log_entry;
    raft_log_->GetEntry(last_applied, &log_entry);
    Status s = Apply(log_entry);
    if (!s.ok()) {
      LOGV(WARN_LEVEL, info_log_, "FloydApply::ApplyStateMachine: Apply log entry failed, at: %d, error: %s",
          last_applied, s.ToString().c_str());
      ScheduleApply();  // try once more
      usleep(1000000);
      return;
    }
  }
  context_->apply_mu.Lock();
  context_->last_applied = last_applied;
  raft_meta_->SetLastApplied(last_applied);
  context_->apply_mu.Unlock();
  context_->apply_cond.SignalAll();
}

Status FloydApply::Apply(const Entry& entry) {
  rocksdb::Status ret;
  switch (entry.optype()) {
    case Entry_OpType_kWrite:
      ret = db_->Put(rocksdb::WriteOptions(), entry.key(), entry.value());
      LOGV(DEBUG_LEVEL, info_log_, "FloydApply::Apply %s, key(%s) value(%s)",
          ret.ToString().c_str(), entry.key().c_str(), entry.value().c_str());
      break;
    case Entry_OpType_kDelete:
      ret = db_->Delete(rocksdb::WriteOptions(), entry.key());
      break;
    case Entry_OpType_kRead:
      ret = rocksdb::Status::OK();
      break;
    default:
      ret = rocksdb::Status::Corruption("Unknown entry type");
  }
  if (!ret.ok()) {
    return Status::Corruption(ret.ToString());
  }
  return Status::OK();
  /* TODO wangkang-xy
  // case CmdRequest::ReadAll:
  // case CmdRequest::TryLock:
  // case CmdRequest::UnLock:
  // case CmdRequest::DeleteUser:
  */
}

} // namespace floyd
