#include "floyd/src/raft_log.h"

#include <vector>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include <google/protobuf/text_format.h>
#include "slash/include/xdebug.h"

#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"
#include "floyd/include/floyd_options.h"

namespace floyd {
extern std::string UintToBitStr(const uint64_t num) {
  char buf[8];
  uint64_t num1 = htobe64(num);
  memcpy(buf, &num1, sizeof(uint64_t));
  return std::string(buf, 8);
}

extern uint64_t BitStrToUint(const std::string &str) {
  uint64_t num;
  memcpy(&num, str.c_str(), sizeof(uint64_t));
  return be64toh(num);
}


RaftLog::RaftLog(rocksdb::DB *db, Logger *info_log) : 
  db_(db),
  info_log_(info_log),
  last_log_index_(0) {
  rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());
  it->SeekToLast();
  if (it->Valid()) {
    it->Prev();
    it->Prev();
    it->Prev();
    it->Prev();
    it->Prev();
    last_log_index_ = BitStrToUint(it->key().ToString());
  }
}

RaftLog::~RaftLog() {
  delete db_;
}

uint64_t RaftLog::Append(const std::vector<Entry *> &entries) {
  slash::MutexLock l(&lli_mutex_);
  std::string buf;
  rocksdb::Status s;
  LOGV(DEBUG_LEVEL, info_log_, "RaftLog::Append: entries.size %lld", entries.size());
  for (size_t i = 0; i < entries.size(); i++) {
    entries[i]->SerializeToString(&buf);
    last_log_index_++;
    s = db_->Put(rocksdb::WriteOptions(), UintToBitStr(last_log_index_), buf);
    if (!s.ok()) {
      LOGV(ERROR_LEVEL, info_log_, "RaftLog::Append false\n");
    }
  }
  return last_log_index_;
}

uint64_t RaftLog::GetLastLogIndex() {
  return last_log_index_;
}

int RaftLog::GetEntry(const uint64_t index, Entry *entry) {
  slash::MutexLock l(&lli_mutex_);
  std::string buf = UintToBitStr(index);
  std::string res;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), buf, &res);
  if (s.IsNotFound()) {
    LOGV(ERROR_LEVEL, info_log_, "RaftLog::GetEntry: GetEntry not found %lld\n", index);
    entry = NULL;
    return 1;
  }
  entry->ParseFromString(res);
  return 0;
}

bool RaftLog::GetLastLogTermAndIndex(uint64_t* last_log_term, uint64_t* last_log_index) {
  slash::MutexLock l(&lli_mutex_);
  if (last_log_index_ == 0) {
    *last_log_index = 0;
    *last_log_term = 0;
    return true;
  }
  std::string buf;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), UintToBitStr(last_log_index_), &buf);
  if (!s.ok() || s.IsNotFound()) {
    *last_log_index = 0;
    *last_log_term = 0;
    return true;
  }
  Entry *entry = new Entry();
  bool is = entry->ParseFromString(buf);
  *last_log_index = last_log_index_;
  *last_log_term = entry->term();
  return true;
}

int RaftLog::TruncateSuffix(uint64_t index) {
  // here we need to delete the unnecessary entry, since we don't store
  // last_log_index in rocksdb
  for (uint64_t i = index; i <= last_log_index_; i++) {
    db_->Delete(rocksdb::WriteOptions(), UintToBitStr(i));
  }
  last_log_index_ = index;
  return 0;
}

}
