#include <iostream>
#include <unistd.h>

#include "db_nemo_impl.h"

#include "rocksdb/db.h"
#include <google/protobuf/text_format.h>

using namespace rocksdb;

void Usage() {
  printf ("Usage:\n"
          "  ./update -i ./log/path/ -o output/path/                 ---- parse all files \n");
}


int main(int argc, char* argv[]) {
  if (argc != 5) {
    Usage();
    exit(-1);
  }

  std::string input_path;
  std::string output_path;
  std::string file;

  char ch;
  while (-1 != (ch = getopt(argc, argv, "i:o:"))) {
    switch (ch) {
      case 'i':
        input_path = optarg;
        printf ("input log path : %s\n", input_path.c_str());
        break;
      case 'o':
        output_path = optarg;
        printf ("output db path : %s\n", output_path.c_str());
        break;
      default:
        Usage();
        exit(-1);
    }
  }

  // Create NemoDB
  rocksdb::DBNemo* input_db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DBNemo::Open(options, input_path, &input_db);
  if (!s.ok()) {
    printf ("Open input nemodb failed! path: %s", input_path.c_str());
    return -1; 
  }

  rocksdb::DB* ouput_db;
  s = rocksdb::DB::Open(options, output_path, &ouput_db);
  if (!s.ok()) {
    printf ("Open output rocksDB failed! path: %s", output_path.c_str());
    return -1; 
  }

  rocksdb::Iterator* iter = input_db->NewIterator(rocksdb::ReadOptions());
  int cnt = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    cnt++;
    printf("%d: key (%s) value (%s)\n", cnt, iter->key().ToString().c_str(), iter->value().ToString().c_str());

    ouput_db->Put(rocksdb::WriteOptions(), iter->key(), iter->value());
  }
  printf("Total %d\n", cnt);
  return 0;
}
