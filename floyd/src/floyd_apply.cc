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


namespace floyd {

FloydApply::FloydApply(FloydContext* context, rocksdb::DB* db, RaftLog* raft_log)
  : context_(context),
    db_(db),
    raft_log_(raft_log) {
  bg_thread_ = new pink::BGThread();
  bg_thread_->set_thread_name("FloydApply");
}

FloydApply::~FloydApply() {
  delete bg_thread_;
}

Status FloydApply::ScheduleApply() {
  if (bg_thread_->StartThread() != 0) {
    return Status::Corruption("Failed to start apply thread");
  }
  bg_thread_->Schedule(&ApplyStateMachine, static_cast<void*>(this));
  return Status::OK();
}

void FloydApply::ApplyStateMachine(void* arg) {
  FloydApply* fapply = static_cast<FloydApply*>(arg);
  FloydContext* context = fapply->context_;

  // Apply as more entry as possible
  uint64_t len = 0, to_apply = 0;
  to_apply = context->NextApplyIndex(&len);

  LOGV(DEBUG_LEVEL, context->info_log(), "FloydApply::ApplyStateMachine: %lu entries to apply from to_apply(%lu)",
            len, to_apply);
  while (len-- > 0) {
    Entry log_entry;
    fapply->raft_log_->GetEntry(to_apply, &log_entry);
    Status s = fapply->Apply(log_entry);
    if (!s.ok()) {
      LOGV(WARN_LEVEL, context->info_log(), "FloydApply::ApplyStateMachine: Apply log entry failed, at: %d, error: %s",
          to_apply, s.ToString().c_str());
      fapply->ScheduleApply();  // try once more
      usleep(1000000);
      return;
    }
    context->ApplyDone(to_apply);
    to_apply++;
  }
  fapply->raft_log_->UpdateLastApplied(to_apply - 1);
}

Status FloydApply::Apply(const Entry& entry) {
  rocksdb::Status ret;
  switch (entry.optype()) {
    case Entry_OpType_kWrite:
      ret = db_->Put(rocksdb::WriteOptions(), entry.key(), entry.value());
      LOGV(DEBUG_LEVEL, context_->info_log(), "FloydApply::Apply %s, key(%s) value(%s)",
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
