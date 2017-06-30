#ifndef FLOYD_RAFT_LOG_H_
#define FLOYD_RAFT_LOG_H_

#include <string>
#include <stdint.h>
#include <atomic>

#include "rocksdb/db.h"

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
                      int32_t voted_for_port, uint64_t apply_index);


  // return persistent state from zeppelin
  uint64_t current_term();
  std::string voted_for_ip();
  int voted_for_port(); 

  uint64_t apply_index() {
    return apply_index_;
  }
  void set_apply_index(uint64_t apply_index) {
    apply_index_ = apply_index;
  }

 private:
  std::string path_;
  std::atomic<uint64_t> index_;
  uint64_t apply_index_;
  rocksdb::DB* log_db_;

  Logger* info_log_;
  RaftLog(const RaftLog&);
  void operator=(const RaftLog&);
};  // RaftLog

}; // namespace floyd

#endif  // FLOYD_RAFT_LOG_H_
