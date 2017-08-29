/**
 * @file t8.cc
 * @brief start three nodes, node1 has the shortest log, and it will start first
 * log struct: (term, key, value)
 * node1 | (22, "22", "22"), (22, "23", "23"), (22, "24", "24"), (22, "25", "25"), (22, "26", 26")
 * node2 | (22, "22", "22"), (22, "23", "23"), (22, "24", "24"), (27, "27key", "27val"), (27, "28key", "28val"), (27, "29key", "29val"), (27, "30key", "30val"), (27, "30key", "30val")
 * node3 | (22, "22", "22"), (22, "23", "23"), (22, "24", "24"), (27, "27key", "27val"), (27, "28key", "28val"), (27, "29key", "29val"), (27, "30key", "30val"), (27, "30key", "30val")
 *
 * after start node2, node3. node2 or node3 will become leader, and node1 will
 * delete these two log (22, "25", "25"), (22, "26", 26")
 * 
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
#include "floyd/include/floyd.h"

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
  InitEntry(22, "22", "22", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(22, "23", "23", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(22, "24", "24", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(22, "25", "25", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(22, "26", "26", entry);
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
  InitEntry(22, "22", "22", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(22, "23", "23", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(22, "24", "24", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "27key", "27val", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "28key", "28val", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "29key", "29val", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "30key", "30val", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "31key", "31val", entry);
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
  InitEntry(22, "22", "22", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(22, "23", "23", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(22, "24", "24", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "27key", "27val", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "28key", "28val", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "29key", "29val", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "30key", "30val", entry);
  entries.push_back(entry);

  entry = new Entry();
  InitEntry(27, "31key", "31val", entry);
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

int main()
{
  cl1("./data1/");
  cl2("./data2/");
  cl3("./data3/");

  Floyd *f1, *f2, *f3;
  slash::Status s;

  Options op1("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903", "127.0.0.1", 8901, "./data1/");
  s = Floyd::Open(op1, &f1);
  printf("%s\n", s.ToString().c_str());

  sleep(2);

  Options op2("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903", "127.0.0.1", 8902, "./data2/");
  s = Floyd::Open(op2, &f2);
  printf("%s\n", s.ToString().c_str());

  Options op3("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903", "127.0.0.1", 8903, "./data3/");
  s = Floyd::Open(op3, &f3);
  printf("%s\n", s.ToString().c_str());

  getchar();

  return 0;
}
