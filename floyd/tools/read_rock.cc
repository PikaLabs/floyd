#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include "rocksdb/db.h"

using namespace rocksdb;

int main(int argc, char**  argv)
{
  rocksdb::DB* db;
  rocksdb::Options options;
  std::cout << argv[1] << std::endl;
  rocksdb::Status s = rocksdb::DB::OpenForReadOnly(options, argv[1], &db);
  rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
  char c;
  int index = 0;
  std::string key = "";
  while ((c = getopt(argc, argv, "i:")) != EOF) {
    switch (c) {
    case 'i':
      key = std::string(optarg, strlen(optarg));
      break;
    }
  }
  if (key != "") {
    std::string val;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
      printf("key %s not found\n", key.c_str());
    } else {
      printf("key %s, val %s\n", key.c_str(), val.c_str());
    }
    return 0;
  }
  int cnt = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    cnt++;
    std::cout << "key " << iter->key().ToString() << "; val " << iter->value().ToString() << std::endl;
  }
  printf("cnt %d\n", cnt);
  return 0;
}
