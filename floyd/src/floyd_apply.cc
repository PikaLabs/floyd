// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_apply.h"

#include <google/protobuf/text_format.h>

#include <unistd.h>
#include <string>

#include "slash/include/xdebug.h"
#include "slash/include/env.h"

#include "floyd/src/logger.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/raft_meta.h"
#include "floyd/src/raft_log.h"

namespace floyd {

FloydApply::FloydApply(FloydContext* context, rocksdb::DB* db, RaftMeta* raft_meta,
    RaftLog* raft_log, Logger* info_log)
  : bg_thread_(1024 * 1024 * 1024),
    context_(context),
    db_(db),
    raft_meta_(raft_meta),
    raft_log_(raft_log),
    info_log_(info_log) {
}

FloydApply::~FloydApply() {
}

int FloydApply::Start() {
  bg_thread_.set_thread_name("FloydApply");
  bg_thread_.Schedule(ApplyStateMachineWrapper, this);
  return bg_thread_.StartThread();
}

int FloydApply::Stop() {
  return bg_thread_.StopThread();
}

void FloydApply::ScheduleApply() {
  /*
   * int timer_queue_size, queue_size;
   * bg_thread_.QueueSize(&timer_queue_size, &queue_size);
   * LOGV(INFO_LEVEL, info_log_, "Peer::AddRequestVoteTask timer_queue size %d queue_size %d",
   *     timer_queue_size, queue_size);
   */
  bg_thread_.Schedule(&ApplyStateMachineWrapper, this);
}

void FloydApply::ApplyStateMachineWrapper(void* arg) {
  reinterpret_cast<FloydApply*>(arg)->ApplyStateMachine();
}

void FloydApply::ApplyStateMachine() {
  uint64_t last_applied = context_->last_applied;
  // Apply as more entry as possible
  uint64_t commit_index;
  commit_index = context_->commit_index;

  LOGV(DEBUG_LEVEL, info_log_, "FloydApply::ApplyStateMachine: last_applied: %lu, commit_index: %lu",
            last_applied, commit_index);
  Entry log_entry;
  if (last_applied >= commit_index) {
    return;
  }
  while (last_applied < commit_index) {
    last_applied++;
    raft_log_->GetEntry(last_applied, &log_entry);
    rocksdb::Status s = Apply(log_entry);
    if (!s.ok()) {
      LOGV(WARN_LEVEL, info_log_, "FloydApply::ApplyStateMachine: Apply log entry failed, at: %d, error: %s",
          last_applied, s.ToString().c_str());
      usleep(1000000);
      ScheduleApply();  // try once more
      return;
    }
  }
  context_->apply_mu.Lock();
  context_->last_applied = last_applied;
  raft_meta_->SetLastApplied(last_applied);
  context_->apply_mu.Unlock();
  context_->apply_cond.SignalAll();
}

rocksdb::Status FloydApply::Apply(const Entry& entry) {
  rocksdb::Status ret;
  Lock lock;
  std::string val;
  switch (entry.optype()) {
    case Entry_OpType_kWrite:
      ret = db_->Put(rocksdb::WriteOptions(), entry.key(), entry.value());
      LOGV(DEBUG_LEVEL, info_log_, "FloydApply::Apply %s, key(%s)",
          ret.ToString().c_str(), entry.key().c_str());
      break;
    case Entry_OpType_kDelete:
      ret = db_->Delete(rocksdb::WriteOptions(), entry.key());
      break;
    case Entry_OpType_kRead:
      ret = rocksdb::Status::OK();
      break;
    case Entry_OpType_kTryLock:
      ret = db_->Get(rocksdb::ReadOptions(), entry.key(), &val);
      if (ret.ok()) {
        lock.ParseFromString(val);
        if (lock.lease_end() < slash::NowMicros()) {
          LOGV(INFO_LEVEL, info_log_, "FloydApply::Apply Trylock Success, name %s holder %s, "
              "but the lock has been locked by %s, and right now it is timeout",
              entry.key().c_str(), entry.holder().c_str(), lock.holder().c_str());
          lock.set_holder(entry.holder());
          lock.set_lease_end(entry.lease_end());
          lock.SerializeToString(&val);
          ret = db_->Put(rocksdb::WriteOptions(), entry.key(), val);
        } else {
          ret = rocksdb::Status::OK();
        }
      } else if (ret.IsNotFound()) {
        lock.set_holder(entry.holder());
        lock.set_lease_end(entry.lease_end());
        lock.SerializeToString(&val);
        ret = db_->Put(rocksdb::WriteOptions(), entry.key(), val);
      } else {
        LOGV(WARN_LEVEL, info_log_, "FloydImpl::Apply Trylock Error operate db error, name %s holder %s",
            entry.key().c_str(), entry.holder().c_str());
      }
      break;
    case Entry_OpType_kUnLock:
      ret = db_->Get(rocksdb::ReadOptions(), entry.key(), &val);
      if (ret.ok()) {
        lock.ParseFromString(val);
        if (lock.holder() != entry.holder()) {
          LOGV(INFO_LEVEL, info_log_, "FloydApply::Apply Warning UnLock an lock holded by other, name %s holder %s, origin holder %s",
              entry.key().c_str(), entry.holder().c_str(), lock.holder().c_str());
        } else if (lock.lease_end() < slash::NowMicros()) {
          LOGV(INFO_LEVEL, info_log_, "FloydImpl::Apply UnLock an lock which is expired, name %s holder %s",
              entry.key().c_str(), entry.holder().c_str(), lock.holder().c_str());
        } else {
          ret = db_->Delete(rocksdb::WriteOptions(), entry.key());
        }
      } else if (ret.IsNotFound()) {
        LOGV(INFO_LEVEL, info_log_, "FloydApply::Apply Warning UnLock an dosen't exist lock, name %s holder %s",
            entry.key().c_str(), entry.holder().c_str());
        ret = rocksdb::Status::OK();
      } else {
        LOGV(WARN_LEVEL, info_log_, "FloydApply::Apply UnLock Error, operate db error, name %s holder %s",
            entry.key().c_str(), entry.holder().c_str());
      }
      break;
    default:
      ret = rocksdb::Status::Corruption("Unknown entry type");
  }
  return ret;
}

} // namespace floyd
