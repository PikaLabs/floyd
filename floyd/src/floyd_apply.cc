#include "floyd/src/floyd_apply.h"

namespace floyd {

FloydApply::FLoydApply(const FloydApplyEnv& env)
  : context_(env.context),
  db_(env.db),
  log_(env.log){
    bg_thread_ = new pink::BGThread();
    BGThread_->set_thread_name("FloydApply");
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
  to_apply = context.NextApplyIndex(&len);
  while (len-- > 0) {
    const Log::Entry& log_entry = fapply->log_->GetEntry(to_apply);
    Status s = fapply->Apply(log_entry);
    if (!s.ok()) {
      LOG_WARNING("Apply log entry failed, at: %d, error: %s",
          to_apply, s.ToString().c_str());
      fapply->ScheduleApply(); // try once more
      usleep(1000);
      return;
    }
    context->ApplyDone(uint64_t index);
    // Notify worker as soon as possible
    fapply->SignalApply();
  }
}

Status FloyApply::Apply(const Log::Entry& log_entry) {
  if (log_entry.type() == raft::Entry::NOOP) {
    return true;
  }

  const std::string& data = log_entry.cmd();
  command::Command cmd;
  if (!cmd.ParseFromArray(data.c_str(), data.length())) {
    LOG_WARNING("Parse log_entry failed");
    return false;
  }

  Status ret;
  switch (cmd.type()) {
    case command::Command::Write:
      ret = db_->Set(cmd.kv().key(), cmd.kv().value());
      break;
    case command::Command::Delete:
      ret = Floyd::db->Delete(cmd.kv().key());
      break;
    case command::Command::Read:
      ret = Status::OK();
      break;
    default:
      ret = Status::Corruption("Unknown cmd type");
  }
  return ret;
  /* TODO wangkang-xy
  // case command::Command::ReadAll:
  // case command::Command::TryLock:
  // case command::Command::UnLock:
  // case command::Command::DeleteUser:
  */
}

}
