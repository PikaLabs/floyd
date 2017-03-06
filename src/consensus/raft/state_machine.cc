#include "state_machine.h"

#include "src/command.pb.h"
#include "include/floyd.h"
#include "include/floyd_util.h"
#include "include/logger.h"

namespace floyd {
namespace raft {

StateMachine::StateMachine(RaftConsensus* raft_con)
    : raft_con_(raft_con),
      apply_state_(&mutex_),
      last_apply_index_(0),
      exiting_(false) {
  apply_ = new ApplyThread(this);
}

StateMachine::~StateMachine() {
  if (!exiting_) Exit();
  apply_->JoinThread();
  delete apply_;
}

void StateMachine::Init() { apply_->StartThread(); }

void StateMachine::Exit() {
  MutexLock l(&mutex_);
  exiting_ = true;
  raft_con_->WakeUpAll();
}

bool StateMachine::WaitForWriteResponse(uint64_t log_index) {
  MutexLock l(&mutex_);
  while (last_apply_index_ < log_index) apply_state_.Wait();
  return true;
}

bool StateMachine::WaitForReadResponse(uint64_t log_index, std::string& key,
                                       std::string& value) {

  MutexLock l(&mutex_);
  while (last_apply_index_ < log_index) apply_state_.Wait();
  LOG_DEBUG("MainThread::WaitForReadResponse: Waked up by ApplyThread");

  floyd::Status ret = floyd::Floyd::db->Get(key, value);
  return true;
}

bool StateMachine::WaitForReadAllResponse(uint64_t log_index, KVMap& kvmap) {

  MutexLock l(&mutex_);
  while (last_apply_index_ < log_index) apply_state_.Wait();

  floyd::Status ret = floyd::Floyd::db->GetAll(kvmap);
  return true;
}

floyd::Status StateMachine::WaitForTryLockResponse(uint64_t log_index) {

  MutexLock l(&mutex_);
  while (last_apply_index_ < log_index) apply_state_.Wait();
  LOG_DEBUG("MainThread::WaitForTryLockResponse: Waked up by ApplyThread");
  std::unordered_map<uint64_t, ApplyResult>::iterator it =
      result_map_.find(log_index);
  if (it != result_map_.end()) {
    // printf ("exec TryLock:apply log done, fetch result now: status(%s)\n",
    // it->second.status_.ToString().c_str());
    return it->second.status_;
  }

  return floyd::Status::Corruption("fetch result failed");
}

floyd::Status StateMachine::WaitForUnLockResponse(uint64_t log_index) {

  MutexLock l(&mutex_);
  while (last_apply_index_ < log_index) apply_state_.Wait();

  std::unordered_map<uint64_t, ApplyResult>::iterator it =
      result_map_.find(log_index);
  if (it != result_map_.end()) {
    // printf ("exec UnLock:apply log done, fetch result now: status(%s)\n",
    // it->second.status_.ToString().c_str());
    return it->second.status_;
  }

  return floyd::Status::Corruption("fetch result failed");
}

floyd::Status StateMachine::WaitForDeleteUserResponse(uint64_t log_index) {

  MutexLock l(&mutex_);
  while (last_apply_index_ < log_index) apply_state_.Wait();

  std::unordered_map<uint64_t, ApplyResult>::iterator it =
      result_map_.find(log_index);
  if (it != result_map_.end()) {
    // printf ("exec DeleteUser:apply log done, fetch result now: status(%s)\n",
    // it->second.status_.ToString().c_str());
    return it->second.status_;
  }

  return floyd::Status::Corruption("fetch result failed");
}

StateMachine::ApplyThread::ApplyThread(StateMachine* sm) : sm_(sm) {}

StateMachine::ApplyThread::~ApplyThread() {}

bool StateMachine::ApplyThread::Apply(StateMachine::Entry& entry) {
  floyd::raft::Entry log_entry = entry.log_entry;
  if (log_entry.type() == floyd::raft::Entry::NOOP) return true;

  const std::string& data = log_entry.cmd();
  command::Command cmd;
  cmd.ParseFromArray(data.c_str(), data.length());
  switch (cmd.type()) {
    case command::Command::Write: {
      command::Command_Kv kv = cmd.kv();
      std::string key = kv.key();
      std::string value = kv.value();
      floyd::Status ret = floyd::Floyd::db->Set(key, value);
      if (!ret.ok())
        return false;
      else
        return true;
    }
    case command::Command::Delete: {
      command::Command_Kv kv = cmd.kv();
      std::string key = kv.key();
      floyd::Status ret = floyd::Floyd::db->Delete(key);
      return ret.ok();
    }
    case command::Command::Read: { return true; }

    case command::Command::ReadAll: { return true; }

    case command::Command::TryLock: {
      std::string key = cmd.kv().key();
      std::string user = SerializeUser(cmd.user().ip(), cmd.user().port());

      StateMachine::ApplyResult result;

      int lock_is_available = floyd::Floyd::db->LockIsAvailable(user, key);
      // printf ("   TryLock LockIsAvailable user(%s) key(%s) %d\n",
      // user.c_str(), key.c_str(), lock_is_available);
      if (lock_is_available == 1) {
        // std::string value = SerializeUser(ip, port);
        floyd::Status ret = floyd::Floyd::db->LockKey(user, key);
        if (!ret.ok()) {
          result.status_ = ret;
        }
      } else if (lock_is_available == 0) {
        result.status_ = floyd::Status::Timeout("already locked");
      }

      // printf ("ApplyThread Lock after apply: index=%ld -->
      // result.status(%s)\n", entry.index, result.status_.ToString().c_str());
      sm_->result_map_.insert(std::make_pair(entry.index, result));
      return true;
    }

    case command::Command::UnLock: {
      std::string key = cmd.kv().key();
      std::string user = SerializeUser(cmd.user().ip(), cmd.user().port());

      StateMachine::ApplyResult result;

      int lock_is_available = floyd::Floyd::db->LockIsAvailable(user, key);

      // printf ("   UnLock LockIsAvailable user(%s) key(%s) %d\n",
      // user.c_str(), key.c_str(), lock_is_available);
      if (lock_is_available == 0) {
        floyd::Status ret = floyd::Floyd::db->UnLockKey(user, key);
        if (!ret.ok()) {
          result.status_ = ret;
        }
      } else if (lock_is_available == 1) {
      } else {
      }

      // std::string value;
      // floyd::Status ret = floyd::Floyd::db->Get(lock_key, value);

      // if (ret.ok()) {
      //  std::string old_ip;
      //  int old_port;

      //  ParseUser(value, &old_ip, &old_port);

      //  if (old_ip == ip && old_port == port) {
      //    ret = floyd::Floyd::db->Delete(lock_key);
      //    if (!ret.ok()) {
      //      result.status_ = ret;
      //    }
      //  }
      //} else {
      //  //not exists lock key
      //}

      // printf ("ApplyThread UnLock after apply: index=%ld -->
      // result.status(%s)\n", entry.index, result.status_.ToString().c_str());
      sm_->result_map_.insert(std::make_pair(entry.index, result));
      return true;
    }

    case command::Command::DeleteUser: {
      std::string user = SerializeUser(cmd.user().ip(), cmd.user().port());

      StateMachine::ApplyResult result;

      floyd::Status ret = floyd::Floyd::db->DeleteUserLocks(user);
      if (!ret.ok()) {
        result.status_ = ret;
      }

      // printf ("ApplyThread DeleteUser after apply: index=%ld -->
      // result.status(%s)\n", entry.index, result.status_.ToString().c_str());
      sm_->result_map_.insert(std::make_pair(entry.index, result));
      return true;
    }
    default:
      return false;
  }
}

void* StateMachine::ApplyThread::ThreadMain() {
  while (!sm_->exiting_) {
    uint64_t last_apply_index;
    {
      MutexLock l(&sm_->mutex_);
      last_apply_index = sm_->last_apply_index_;
    }
    StateMachine::Entry entry =
        sm_->raft_con_->TryGetNextCommitEntry(last_apply_index);
    if (entry.index == last_apply_index) {
      sm_->raft_con_->Wait();
      continue;
    } else if (entry.index == last_apply_index + 1) {
      if (Apply(entry)) {
        MutexLock l(&sm_->mutex_);
        sm_->last_apply_index_ = entry.index;
        LOG_DEBUG(
            "ApplyThread::ThreadMain: Waked up by AdvanceCommitIndex, "
            "last_apply_index_=%d",
            sm_->last_apply_index_);
        sm_->apply_state_.SignalAll();
      }
      continue;
    } else {
    }
  }

  return NULL;
}

}  // namespace raft
}  // namespace floyd
