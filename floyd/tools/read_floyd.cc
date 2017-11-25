#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include "rocksdb/db.h"
#include "floyd/src/floyd.pb.h"
#include <google/protobuf/text_format.h>

using namespace rocksdb;
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

int main(int argc, char** argv)
{
  rocksdb::DB* db;
  rocksdb::Options options;
  std::cout << argv[1] << std::endl;
  rocksdb::Status s = rocksdb::DB::OpenForReadOnly(options, argv[1], &db);
  rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
  char c;
  bool is_meta = false;
  int index = 0;
  floyd::Entry entry;
  while ((c = getopt(argc, argv, "mi:")) != EOF) {
    switch (c) {
      case 'm':
        is_meta = true;
        break;
      case 'i':
        index = atoi(optarg);
    }
  }
  if (is_meta) {
    iter->SeekToLast();
    for (int i = 0; i < 5; i++) {
      if (iter->key().ToString() == "VOTEFORIP") {
        printf("key %s, value %s\n", iter->key().ToString().c_str(), iter->value().ToString().c_str());
      } else {
        uint64_t ans;
        memcpy(&ans, iter->value().data(), sizeof(uint64_t));
        printf("key %s, value %lu\n", iter->key().ToString().c_str(), ans);
      }
      iter->Prev();
    }
    return 0;
  }
  if (index) {
    std::string val;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), UintToBitStr(uint64_t(index)), &val);
    if (s.IsNotFound()) {
      printf("key %d not found\n", index);
    } else {
      entry.ParseFromString(val);
      printf("index %d entry term: %lu key %s value %s\n", index, entry.term(), entry.key().c_str(), entry.value().c_str());
    }
    return 0;
  }
  int cnt = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    cnt++;
    if (iter->key().ToString() == "CURRENTTERM" || iter->key().ToString() == "VOTEFORIP" || iter->key().ToString() == "VOTEFORPORT" || iter->key().ToString() == "APPLYINDEX" || iter->key().ToString() == "COMMITINDEX" || iter->key().ToString() == "FENCINGTOKEN") {
      if (iter->key().ToString() == "VOTEFORIP") {
        printf("key %s, value %s\n", iter->key().ToString().c_str(), iter->value().ToString().c_str());
      } else {
        uint64_t ans;
        memcpy(&ans, iter->value().data(), sizeof(uint64_t));
        printf("key %s, value %lu\n", iter->key().ToString().c_str(), ans);
      }
    } else {
      entry.ParseFromString(iter->value().ToString());
      uint64_t num = BitStrToUint(iter->key().ToString());
      if (entry.optype() == floyd::Entry_OpType_kTryLock) {
        printf("index %lu entry type: trylock term: %lu name: %s holder %s\n", num, entry.term(), entry.key().c_str(), entry.holder().c_str());
      } else if (entry.optype() == floyd::Entry_OpType_kUnLock) {
        printf("index %lu entry type: unlock term: %lu name: %s holder %s\n", num, entry.term(), entry.key().c_str(), entry.holder().c_str());
      } else if (entry.optype() == floyd::Entry_OpType_kAddServer) {
        printf("index %lu entry type: addserver new_server: %s\n", num, entry.new_server().c_str());
      } else {
        printf("index %lu entry type: %d term: %lu key %s value %s\n", num, entry.optype(), entry.term(), entry.key().c_str(), entry.value().c_str());
      }
    }
    // std::cout << "res " << iter->key().ToString() << ": " << iter->value().ToString() << std::endl;
  }
  printf("cnt %d\n", cnt);
  return 0;
}
