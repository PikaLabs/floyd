#include <iostream>

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
    floyd::Entry entry;
    if (iter->key().ToString() == "CURRENTTERM" || iter->key().ToString() == "VOTEFORIP" || iter->key().ToString() == "VOTEFORPORT" || iter->key().ToString() == "APPLYINDEX") {
      continue;
    }
    entry.ParseFromString(iter->value().ToString());
    floyd::CmdRequest cmd_request;
    cmd_request.ParseFromString(entry.cmd());
    uint64_t num = BitStrToUint(iter->key().ToString());
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(cmd_request, &text_format);
    printf("cmd_request :\n%s \n", text_format.c_str());

    printf("key %lld entry term: %d key %s value %s\n", num, entry.term(), cmd_request.kv().key().c_str(), cmd_request.kv().value().c_str());
    // std::cout << "res " << iter->key().ToString() << ": " << iter->value().ToString() << std::endl;
  }
  printf("cnt %d\n", cnt);
  return 0;
}
