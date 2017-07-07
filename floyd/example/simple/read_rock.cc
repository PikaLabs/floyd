#include <iostream>
#include "rocksdb/db.h"

using namespace rocksdb;

int main(int argc, char**  argv)
{
  rocksdb::DB* db;
  rocksdb::Options options;
  std::cout << argv[1] << std::endl;
  rocksdb::Status s = rocksdb::DB::Open(options, argv[1], &db);
  rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
  int cnt = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    cnt++;
    std::cout << "res " << iter->key().ToString() << ": " << iter->value().ToString() << std::endl;
  }
  printf("cnt %d\n", cnt);
  return 0;
}
