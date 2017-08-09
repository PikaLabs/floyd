// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/raft_log.h"

#include <google/protobuf/text_format.h>

#include <vector>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
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
    if (it->Valid()) {
      last_log_index_ = BitStrToUint(it->key().ToString());
    }
  }
  delete it;
}

RaftLog::~RaftLog() {
}

uint64_t RaftLog::Append(const std::vector<const Entry *> &entries) {
  slash::MutexLock l(&lli_mutex_);
  rocksdb::WriteBatch wb;
  LOGV(DEBUG_LEVEL, info_log_, "RaftLog::Append: entries.size %lld", entries.size());
  for (size_t i = 0; i < entries.size(); i++) {
    std::string buf;
    entries[i]->SerializeToString(&buf);
    last_log_index_++;
    wb.Put(UintToBitStr(last_log_index_), buf);
    // s = db_->Put(rocksdb::WriteOptions(), UintToBitStr(last_log_index_), buf);
    // if (!s.ok()) {
    //   LOGV(ERROR_LEVEL, info_log_, "RaftLog::Append %lu string %s false\n", last_log_index_, UintToBitStr(last_log_index_).c_str());
    //   return --last_log_index_;
    // }
  }
  rocksdb::Status s;
  s = db_->Write(rocksdb::WriteOptions(), &wb);
  if (!s.ok()) {
    LOGV(ERROR_LEVEL, info_log_, "RaftLog::Append %lu false\n", last_log_index_);
    last_log_index_ -= entries.size();
    return last_log_index_;
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
    LOGV(ERROR_LEVEL, info_log_, "RaftLog::GetEntry: GetEntry not found %lld \n", index);
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
  Entry entry;
  entry.ParseFromString(buf);
  *last_log_index = last_log_index_;
  *last_log_term = entry.term();
  return true;
}

/*
 * truncate suffix from index
 */
int RaftLog::TruncateSuffix(uint64_t index) {
  // we need to delete the unnecessary entry, since we don't store
  // last_log_index in rocksdb
  for (; last_log_index_ >= index; last_log_index_--) {
    rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), UintToBitStr(last_log_index_));
    if (!s.ok()) {
      LOGV(ERROR_LEVEL, info_log_, "RaftLog::TruncateSuffix Error last_log_index %lu "
          "truncate from %lu\n", last_log_index_, index);
      return -1;
    }
  }
  return 0;
}

}  // namespace floyd
