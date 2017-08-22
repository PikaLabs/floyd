#include <iostream>
#include <stdio.h>
#include "rocksdb/db.h"

using namespace rocksdb;

int main(int argc, char**  argv)
{
  rocksdb::DB *db1, *db2;
  rocksdb::Options options;
  if (argc < 3) {
    printf("need three parameter\n");
    return -1;
  }
  std::cout << argv[1] << " " << argv[2] << std::endl;
  rocksdb::Status s1 = rocksdb::DB::OpenForReadOnly(options, argv[1], &db1);
  rocksdb::Status s2 = rocksdb::DB::OpenForReadOnly(options, argv[2], &db2);
  if (!s1.ok() || !s2.ok()) {
    printf("failed, open db error\n");
    return -1;
  }

  rocksdb::Iterator* iter1 = db1->NewIterator(rocksdb::ReadOptions());
  rocksdb::Iterator* iter2 = db2->NewIterator(rocksdb::ReadOptions());
  iter1->SeekToFirst();
  iter2->SeekToFirst();
  if (!iter1->Valid() || !iter2->Valid()) {
    printf("failed, newiterator error\n");
    return -1;
  }

  while (iter1->Valid() && iter2->Valid()) {
    if (iter1->key() != iter2->key() || iter1->value() != iter2->value()) {
      printf("failed, two rocksdb data not the same\n");
      return -1;
    }
    iter1->Next();
    iter2->Next();
  }
  if (iter1->Valid() || iter2->Valid()) {
    printf("failed, two rocksdb data not the same\n");
    return -1;
  }
  printf("success, two rocksdb data are same\n");
  delete iter2;
  delete iter1;
  delete db2;
  delete db1;
  return 0;
}
