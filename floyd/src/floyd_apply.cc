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

  LOGV(DEBUG_LEVEL, context->info_log(), "ApplyStateMachine with %lu entries to apply from to_apply(%lu)",
            len, to_apply);
  while (len-- > 0) {
    Entry log_entry;
    fapply->raft_log_->GetEntry(to_apply, &log_entry);
    Status s = fapply->Apply(log_entry);
    if (!s.ok()) {
      LOGV(WARN_LEVEL, context->info_log(), "Apply log entry failed, at: %d, error: %s",
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

Status FloydApply::Apply(const Entry& log_entry) {
  const std::string& data = log_entry.cmd();
  CmdRequest cmd;
  if (!cmd.ParseFromArray(data.c_str(), data.length())) {
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(cmd, &text_format);
    LOGV(WARN_LEVEL, context_->info_log(), "FloydApply:Apply :\n%s \n", text_format.c_str());
    LOGV(WARN_LEVEL, context_->info_log(), "Parse log_entry failed");
    return Status::IOError("Parse error");
  }

  rocksdb::Status ret;
  switch (cmd.type()) {
    case Type::Write:
      ret = db_->Put(rocksdb::WriteOptions(), cmd.kv().key(), cmd.kv().value());
      LOGV(DEBUG_LEVEL, context_->info_log(), "Floyd Apply Write %s, key(%s) value(%s)",
          ret.ToString().c_str(), cmd.kv().key().c_str(), cmd.kv().value().c_str());
      break;
    case Type::Delete:
      ret = db_->Delete(rocksdb::WriteOptions(), cmd.kv().key());
      break;
    case Type::Read:
      ret = rocksdb::Status::OK();
      break;
    default:
      ret = rocksdb::Status::Corruption("Unknown cmd type");
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
