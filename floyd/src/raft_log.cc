#include "floyd/src/raft_log.h"

#include <vector>
#include <string>

#include "rocksdb/db.h"
#include <google/protobuf/text_format.h>
#include "slash/include/xdebug.h"

#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"
#include "floyd/include/floyd_options.h"

namespace floyd {

static const std::string kCurrentTerm = ".CURRENTTERM";
static const std::string kVoteForIp = ".VOTEFORIP";
static const std::string kVoteForPort = ".VOTEFORPORT";
static const std::string kApplyIndex = ".APPLYINDEX";

RaftLog::RaftLog(const std::string &path, Logger *info_log) : 
  info_log_(info_log),
  index_(0), 
  apply_index_(0) {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &log_db_);
  assert(status.ok());
  // TODO(ba0tiao) add seek to max to initialize index_
}

RaftLog::~RaftLog() {
  delete log_db_;
}

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

uint64_t RaftLog::Append(const std::vector<Entry *> &entries) {
  std::string buf;
  rocksdb::Status s;
  LOGV(DEBUG_LEVEL, info_log_, "entries.size %lld", entries.size());
  for (int i = 0; i < entries.size(); i++) {
    entries[i]->SerializeToString(&buf);
    index_++;
    s = log_db_->Put(rocksdb::WriteOptions(), UintToBitStr(index_), buf);
    if (!s.ok()) {
      printf("RaftLog::Append false\n");
    }
  }
  log_info("RaftLog::Append %lld", index_.load());
  return index_;
}

uint64_t RaftLog::GetLastLogIndex() {
  return index_;
}

int RaftLog::GetEntry(uint64_t index, Entry *entry) {
  std::string buf = UintToBitStr(index);
  std::string res;
  rocksdb::Status s = log_db_->Get(rocksdb::ReadOptions(), buf, &res);
  if (s.IsNotFound()) {
    log_info("GetEntry not found %lld\n", index);
    entry = NULL;
    return 0;
  }
  entry->ParseFromString(res);
  return 1;
}

uint64_t RaftLog::current_term() {
  std::string buf;
  uint64_t ans;
  rocksdb::Status s = log_db_->Get(rocksdb::ReadOptions(), kCurrentTerm, &buf);
  printf("status %s\n", s.getState());
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
  if (index_ == 0) {
    *last_log_index = 0;
    *last_log_term = 0;
    return true;
  }
  std::string buf;
  rocksdb::Status s = log_db_->Get(rocksdb::ReadOptions(), UintToBitStr(index_), &buf);
  if (!s.ok() || s.IsNotFound()) {
    *last_log_index = 0;
    *last_log_term = 0;
    return true;
  }
  Entry *entry = new Entry();
  bool is = entry->ParseFromString(buf);
  *last_log_index = index_;
  *last_log_term = entry->term();
  return true;
}

void RaftLog::UpdateMetadata(uint64_t current_term, std::string voted_for_ip,
                      int32_t voted_for_port, uint64_t apply_index) {
  char buf[8];
  memcpy(buf, &current_term, sizeof(uint64_t));
  log_db_->Put(rocksdb::WriteOptions(), kCurrentTerm, std::string(buf, 8));
  log_db_->Put(rocksdb::WriteOptions(), kVoteForIp, voted_for_ip);
  memcpy(buf, &voted_for_port, sizeof(uint32_t));
  log_db_->Put(rocksdb::WriteOptions(), kVoteForPort, std::string(buf, 4));
  memcpy(buf, &apply_index, sizeof(uint64_t));
  log_db_->Put(rocksdb::WriteOptions(), kApplyIndex, std::string(buf, 8));
}

}
