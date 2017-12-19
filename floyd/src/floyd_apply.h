// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef FLOYD_SRC_FLOYD_APPLY_H_
#define FLOYD_SRC_FLOYD_APPLY_H_

#include "floyd/src/floyd_context.h"

#include "slash/include/slash_status.h"
#include "pink/include/bg_thread.h"

namespace floyd {

using slash::Status;

class RaftMeta;
class RaftLog;
class Logger;
class FloydImpl;

class FloydApply {
 public:
  FloydApply(FloydContext* context, rocksdb::DB* db, RaftMeta* raft_meta,
      RaftLog* raft_log, FloydImpl* impl_, Logger* info_log); 
  virtual ~FloydApply();
  int Start();
  int Stop();
  void ScheduleApply();

 private:
  pink::BGThread bg_thread_;
  FloydContext* const context_;
  rocksdb::DB* const db_;
  /*
   * we will store the increasing id in raft_meta_
   */
  RaftMeta* const raft_meta_;
  RaftLog* const raft_log_;
  FloydImpl* const impl_;
  Logger* const info_log_;
  static void ApplyStateMachineWrapper(void* arg);
  void ApplyStateMachine();
  rocksdb::Status Apply(const Entry& log_entry);
  rocksdb::Status MembershipChange(const std::string& ip_port, bool add);


  FloydApply(const FloydApply&);
  void operator=(const FloydApply&);
};

}  // namespace floyd

#endif  // FLOYD_SRC_FLOYD_APPLY_H_
