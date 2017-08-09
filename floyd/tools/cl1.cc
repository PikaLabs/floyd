/**
 * @file cl1.cc
 * @brief construct raft log with different log entry
 * @author chenzongzhi
 * @version 1.0.0
 * @date 2017-08-06
 */


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
int cl1(const std::string path) {
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
  Entry *entry = new Entry();
  InitEntry(1, "1", "1", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "2", "2", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "3", "3", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "4", "4", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "5", "5", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(5, "6", "6", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(5, "7", "7", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "8", "8", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "9", "9", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "10", "10", entry);
  entries.push_back(entry);

  raft_log->Append(entries);
  raft_meta->SetCurrentTerm(6);
  raft_meta->SetCommitIndex(9);
  raft_meta->SetLastApplied(0);

  delete raft_meta;
  delete raft_log;
  delete db;
  delete logger;
}

int cl2(const std::string path) {
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
  Entry *entry = new Entry();
  InitEntry(1, "1", "1", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "2", "2", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "3", "3", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "4", "4", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "5", "5", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(5, "6", "6", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(5, "7", "7", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "8", "8", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "9", "9", entry);
  entries.push_back(entry);

  raft_log->Append(entries);
  raft_meta->SetCurrentTerm(6);
  raft_meta->SetCommitIndex(9);
  raft_meta->SetLastApplied(0);

  delete raft_meta;
  delete raft_log;
  delete db;
  delete logger;
}

int cl3(const std::string path) {
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
  Entry *entry = new Entry();
  InitEntry(1, "1", "1", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "2", "2", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "3", "3", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "4", "4", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "5", "5", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(5, "6", "6", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(5, "7", "7", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "8", "8", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "9", "9", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "10", "10", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(6, "11", "11", entry);
  entries.push_back(entry);

  raft_log->Append(entries);
  raft_meta->SetCurrentTerm(6);
  raft_meta->SetCommitIndex(9);
  raft_meta->SetLastApplied(0);

  delete raft_meta;
  delete raft_log;
  delete db;
  delete logger;
}


int cl4(const std::string path) {
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
  Entry *entry = new Entry();
  InitEntry(1, "1", "1", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "2", "2", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "3", "3", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "4", "4", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "5", "5", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "6", "6", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(4, "7", "7", entry);
  entries.push_back(entry);

  raft_log->Append(entries);
  raft_meta->SetCurrentTerm(4);
  raft_meta->SetCommitIndex(3);
  raft_meta->SetLastApplied(0);

  delete raft_meta;
  delete raft_log;
  delete db;
  delete logger;
}

int cl5(const std::string path) {
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
  Entry *entry = new Entry();
  InitEntry(1, "1", "1", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "2", "2", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(1, "3", "3", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(2, "4", "4", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(2, "5", "5", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(2, "6", "6", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(3, "7", "7", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(3, "8", "8", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(3, "9", "9", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(3, "10", "10", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(3, "11", "11", entry);
  entries.push_back(entry);

  raft_log->Append(entries);
  raft_meta->SetCurrentTerm(3);
  raft_meta->SetCommitIndex(3);
  raft_meta->SetLastApplied(0);

  delete raft_meta;
  delete raft_log;
  delete db;
  delete logger;
}

int main()
{

  cl1("./data1/");
  cl2("./data2/");
  cl3("./data3/");
  cl4("./data4/");
  cl5("./data5/");

  return 0;
}
