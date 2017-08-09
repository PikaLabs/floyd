// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef FLOYD_SRC_RAFT_LOG_H_
#define FLOYD_SRC_RAFT_LOG_H_

#include <stdint.h>

#include <atomic>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

class Logger;
class Entry;

class RaftLog {
 public:
  RaftLog(rocksdb::DB* db, Logger* info_log);
  ~RaftLog();

  uint64_t Append(const std::vector<const Entry *> &entries);

  int GetEntry(uint64_t index, Entry *entry);

  uint64_t GetLastLogIndex();
  bool GetLastLogTermAndIndex(uint64_t* last_log_term, uint64_t* last_log_index);
  int TruncateSuffix(uint64_t index);

 private:
  rocksdb::DB* const db_;
  Logger* info_log_;
  /*
   * mutex for last_log_index_
   */
  slash::Mutex lli_mutex_;
  uint64_t last_log_index_;

  /*
   * we don't store last_log_index_ in rocksdb, since if we store it in rocksdb
   * we need update it every time I append an entry.
   * so we need update it when we open db
   */
  RaftLog(const RaftLog&);
  void operator=(const RaftLog&);
};  // RaftLog

}; // namespace floyd

#endif  // FLOYD_SRC_RAFT_LOG_H_
