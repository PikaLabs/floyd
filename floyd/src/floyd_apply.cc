#include "floyd/src/floyd_apply.h"

#include <unistd.h>
#include "floyd/src/logger.h"
#include "floyd/src/command.pb.h"


namespace floyd {

FloydApply::FloydApply(const FloydApplyEnv& env)
  : context_(env.context),
    db_(env.db),
    log_(env.log) {
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
  while (len-- > 0) {
    const Log::Entry& log_entry = fapply->log_->GetEntry(to_apply);
    Status s = fapply->Apply(log_entry);
    if (!s.ok()) {
      LOG_WARN("Apply log entry failed, at: %d, error: %s",
          to_apply, s.ToString().c_str());
      fapply->ScheduleApply(); // try once more
      usleep(1000);
      return;
    }
    context->ApplyDone(to_apply);
    to_apply++;
    // Notify worker as soon as possible
    context->SignalApply();
  }
}

Status FloydApply::Apply(const Log::Entry& log_entry) {
  const std::string& data = log_entry.cmd();
  command::Command cmd;
  if (!cmd.ParseFromArray(data.c_str(), data.length())) {
    LOG_WARN("Parse log_entry failed");
    return Status::IOError("Parse error");
  }

  rocksdb::Status ret;
  switch (cmd.type()) {
    case command::Command::Write:
      ret = db_->Put(rocksdb::WriteOptions(), cmd.kv().key(), cmd.kv().value());
      LOG_DEBUG("Floyd Apply Write %s, key(%s) value(%s)",
          ret.ToString().c_str(), cmd.kv().key().c_str(), cmd.kv().value().c_str());
      break;
    case command::Command::Delete:
      ret = db_->Delete(rocksdb::WriteOptions(), cmd.kv().key());
      break;
    case command::Command::Read:
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
  // case command::Command::ReadAll:
  // case command::Command::TryLock:
  // case command::Command::UnLock:
  // case command::Command::DeleteUser:
  */
}

} // namespace floyd
