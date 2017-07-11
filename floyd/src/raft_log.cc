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

static const std::string kCurrentTerm = "CURRENTTERM";
static const std::string kVoteForIp = "VOTEFORIP";
static const std::string kVoteForPort = "VOTEFORPORT";
static const std::string kApplyIndex = "APPLYINDEX";

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


RaftLog::RaftLog(const std::string &path, Logger *info_log) : 
  last_log_index_(0), 
  last_applied_(0),
  info_log_(info_log) {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DB::Open(options, path, &log_db_);
  assert(s.ok());
  
  rocksdb::Iterator *it = log_db_->NewIterator(rocksdb::ReadOptions());
  // skip currentterm, voteforip, voteforport, applyindex
  it->SeekToLast();
  if (it->Valid()) {
    it->Prev();
    it->Prev();
    it->Prev();
    it->Prev();
    last_log_index_ = BitStrToUint(it->key().ToString());
  }

  std::string res;
  s = log_db_->Get(rocksdb::ReadOptions(), kApplyIndex, &res);
  if (s.ok()) {
    memcpy(&last_applied_, res.data(), sizeof(uint64_t));
  }
}

RaftLog::~RaftLog() {
  delete log_db_;
}

uint64_t RaftLog::Append(const std::vector<Entry *> &entries) {
  slash::MutexLock l(&lli_mutex_);
  std::string buf;
  rocksdb::Status s;
  LOGV(DEBUG_LEVEL, info_log_, "entries.size %lld", entries.size());
  for (size_t i = 0; i < entries.size(); i++) {
    entries[i]->SerializeToString(&buf);
    last_log_index_++;
    s = log_db_->Put(rocksdb::WriteOptions(), UintToBitStr(last_log_index_), buf);
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
  rocksdb::Status s = log_db_->Get(rocksdb::ReadOptions(), buf, &res);
  if (s.IsNotFound()) {
    LOGV(ERROR_LEVEL, info_log_, "RaftLog::GetEntry: GetEntry not found %lld\n", index);
    entry = NULL;
    return 1;
  }
  entry->ParseFromString(res);
  return 0;
}

uint64_t RaftLog::current_term() {
  std::string buf;
  uint64_t ans;
  rocksdb::Status s = log_db_->Get(rocksdb::ReadOptions(), kCurrentTerm, &buf);
  if (s.IsNotFound()) {
    return 0;
  }
  memcpy(&ans, buf.data(), sizeof(uint64_t));
  return ans;
}

std::string RaftLog::voted_for_ip() {
  std::string buf;
  rocksdb::Status s = log_db_->Get(rocksdb::ReadOptions(), kVoteForIp, &buf);
  if (s.IsNotFound()) {
    return std::string("");
  }
  return buf;
}

int RaftLog::voted_for_port() {
  std::string buf;
  int ans;
  rocksdb::Status s = log_db_->Get(rocksdb::ReadOptions(), kVoteForPort, &buf);
  if (s.IsNotFound()) {
    return 0;
  }
  memcpy(&ans, buf.data(), sizeof(int));
  return ans;
}

bool RaftLog::GetLastLogTermAndIndex(uint64_t* last_log_term, uint64_t* last_log_index) {
  slash::MutexLock l(&lli_mutex_);
  if (last_log_index_ == 0) {
    *last_log_index = 0;
    *last_log_term = 0;
    return true;
  }
  std::string buf;
  rocksdb::Status s = log_db_->Get(rocksdb::ReadOptions(), UintToBitStr(last_log_index_), &buf);
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

void RaftLog::UpdateMetadata(uint64_t current_term, std::string voted_for_ip,
                      int32_t voted_for_port, uint64_t last_applied) {
  char buf[8];
  memcpy(buf, &current_term, sizeof(uint64_t));
  log_db_->Put(rocksdb::WriteOptions(), kCurrentTerm, std::string(buf, 8));
  log_db_->Put(rocksdb::WriteOptions(), kVoteForIp, voted_for_ip);
  memcpy(buf, &voted_for_port, sizeof(uint32_t));
  log_db_->Put(rocksdb::WriteOptions(), kVoteForPort, std::string(buf, 4));
}

void RaftLog::UpdateLastApplied(uint64_t last_applied) {
  last_applied_ = last_applied;
  char buf[8];
  memcpy(buf, &last_applied, sizeof(uint64_t));
  log_db_->Put(rocksdb::WriteOptions(), kApplyIndex, std::string(buf, 8));
}

int RaftLog::TruncateSuffix(uint64_t index) {
  // here we need to delete the unnecessary entry, since we don't store
  // last_log_index in rocksdb
  for (uint64_t i = index; i <= last_log_index_; i++) {
    log_db_->Delete(rocksdb::WriteOptions(), UintToBitStr(i));
  }
  last_log_index_ = index;
  return 0;
}

}
