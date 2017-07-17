#include <iostream>

#include "rocksdb/db.h"
#include "floyd/src/floyd.pb.h"
#include <google/protobuf/text_format.h>

static const std::string kCurrentTerm = "CURRENTTERM";
static const std::string kVoteForIp = "VOTEFORIP";
static const std::string kVoteForPort = "VOTEFORPORT";
static const std::string kApplyIndex = "APPLYINDEX";

int main()
{
  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::Status s = rocksdb::DB::Open(options, "./data1/log", &db);
  log_db_->Put(rocksdb::WriteOptions(), kVoteForPort, std::string(buf, 4));
  return 0;
}
