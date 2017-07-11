// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef FLOYD_SRC_RAFT_LOG_H_
#define FLOYD_SRC_RAFT_LOG_H_

#include <string>
#include <stdint.h>
#include <atomic>

#include "rocksdb/db.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

class Logger;
class Entry;

class RaftLog {
 public:
  RaftLog(const std::string &path, Logger* info_log);
  ~RaftLog();
  uint64_t Append(const std::vector<Entry *> &entries);

  uint64_t GetLastLogIndex();

  int GetEntry(uint64_t index, Entry *entry);

  bool GetLastLogTermAndIndex(uint64_t* last_log_term, uint64_t* last_log_index);

  void UpdateMetadata(uint64_t current_term, std::string voted_for_ip,
                      int32_t voted_for_port, uint64_t last_applied);
  int TruncateSuffix(uint64_t index);

  // return persistent state from zeppelin
  uint64_t current_term();
  std::string voted_for_ip();
  int voted_for_port(); 

  uint64_t last_applied() {
    return last_applied_;
  }
  void UpdateLastApplied(uint64_t last_applied);

  uint64_t last_log_index() {
    return last_log_index_;
  }

 private:
  std::string path_;

  /*
   * mutex for last_log_index_
   */
  slash::Mutex lli_mutex_;

  /*
   * we don't store last_log_index_ in rocksdb, since if we store it in rocksdb
   * we need update it every time I append an entry.
   * so we need update it when we open db
   */
  uint64_t last_log_index_;
  uint64_t last_applied_;
  rocksdb::DB* log_db_;

  Logger* info_log_;
  RaftLog(const RaftLog&);
  void operator=(const RaftLog&);
};  // RaftLog

}; // namespace floyd

#endif  // FLOYD_RAFT_LOG_H_
