#include <iostream>
#include <vector>

#include <google/protobuf/text_format.h>

#include "rocksdb/db.h"
#include "slash/include/env.h"

#include "floyd/src/raft_log.h"
#include "floyd/src/raft_meta.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"

using namespace floyd;
void InitEntry(int term, const std::string &key, const std::string &val, Entry *entry) {
  entry->set_term(term);
  entry->set_key(key);
  entry->set_value(val);
  entry->set_optype(Entry_OpType_kWrite);
}
int cl(const std::string path) {
  // construct data1 node
  Logger *logger;
  slash::CreatePath(path);
  if (NewLogger(path + "/LOG", &logger) != 0) {
    return -1;
  }
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DB::Open(options, path + "/log/", &db);
  RaftMeta *raft_meta = new RaftMeta(db, logger);
  raft_meta->Init();
  RaftLog *raft_log = new RaftLog(db, logger);
  std::vector<const Entry *> entries;
  char buff[10];
  for (int i = 0; i < 10; i++) {
    Entry *entry = new Entry();
    snprintf(buff, sizeof(buff), "%d", i);
    InitEntry(10, std::string(buff, sizeof(int)), std::string(buff, sizeof(int)), entry);
    entries.push_back(entry);
  }
  raft_log->Append(entries);
  raft_meta->SetCurrentTerm(10);
  raft_meta->SetCommitIndex(10);
  raft_meta->SetLastApplied(0);

  delete raft_meta;
  delete raft_log;
  delete db;
  delete logger;
}

int cl_spec(const std::string path) {
  // construct data1 node
  Logger *logger;
  slash::CreatePath(path);
  if (NewLogger(path + "/LOG", &logger) != 0) {
    return -1;
  }
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DB::Open(options, path + "/log/", &db);
  RaftMeta *raft_meta = new RaftMeta(db, logger);
  raft_meta->Init();
  RaftLog *raft_log = new RaftLog(db, logger);
  std::vector<const Entry *> entries;
  char buff[10];
  for (int i = 0; i < 21; i++) {
    Entry *entry = new Entry();
    snprintf(buff, sizeof(buff), "%d", i);
    entry->set_term(10);
    entry->set_key(std::string(buff, sizeof(int)));
    entry->set_value(std::string(buff, sizeof(int)));
    entry->set_optype(Entry_OpType_kWrite);
    entries.push_back(entry);
  }
  raft_log->Append(entries);
  raft_meta->SetCurrentTerm(10);
  raft_meta->SetCommitIndex(10);
  raft_meta->SetLastApplied(0);

  delete raft_meta;
  delete raft_log;
  delete db;
  delete logger;
}

int main()
{

  cl("./data1/");
  cl("./data2/");
  cl("./data3/");
  cl_spec("./data4/");
  cl("./data5/");

  return 0;
}
