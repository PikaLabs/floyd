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

class FloydApply {
 public:
  FloydApply(FloydContext* context, rocksdb::DB* db, RaftLog* raft_log);
  ~FloydApply();
  Status ScheduleApply();

 private:
  pink::BGThread* bg_thread_;
  FloydContext* context_;
  rocksdb::DB* db_;
  RaftLog* raft_log_;
  static void ApplyStateMachine(void* arg);
  Status Apply(const Entry& log_entry);
};

}  // namespace floyd

#endif  // FLOYD_SRC_FLOYD_APPLY_H_
