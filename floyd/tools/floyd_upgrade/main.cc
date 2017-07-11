#include <iostream>
#include <google/protobuf/text_format.h>
#include "file_log.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace std;

void Usage() {
  printf ("Usage:\n"
          "  ./update -i ./log/path/ -o output/path/                 ---- parse all files \n"
          "  ./update -i ./log/path/ -o output/path/ -c cnt          ---- parse the first cnt files \n"
          "  ./update -i ./log/path/ -o output/path/ -f pecific_file ---- parse single file\n"
          "  ./update -i ./log/path/ -o output/path/ -r index        ---- parse specific index entry\n");
}

rocksdb::DB* db;

int main(int argc, char* argv[]) {
  if (argc != 5 && argc != 7) {
    Usage();
    exit(-1);
  }

  std::string input_path;
  std::string output_path;
  std::string file;
  int cnt = 10000;
  int index = -1;

  char ch;
  while (-1 != (ch = getopt(argc, argv, "i:o:f:c:r:"))) {
    switch (ch) {
      case 'i':
        input_path = optarg;
        printf ("input log path : %s\n", input_path.c_str());
        break;
      case 'o':
        output_path = optarg;
        printf ("output db path : %s\n", output_path.c_str());
        break;
      case 'c': {
        char *pend;
        cnt = std::strtol(optarg, &pend, 10);
        printf ("User cnt : %d\n", cnt);
        break;
      }
      case 'f':
        file = optarg;
        printf ("User specify log file : %s\n", file.c_str());
        break;
      case 'r': {
        char *pend;
        index = std::strtol(optarg, &pend, 10);
        printf ("User specify index : %d\n", index);
        break;
      }
      default:
        Usage();
        exit(-1);
    }
  }

  // Create DB
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DB::Open(options, output_path, &db);
  if (!s.ok()) {
    printf ("Open db failed! output path: %s", output_path.c_str());
    return -1; 
  }

  floyd::raft::FileLog* file_log = new floyd::raft::FileLog(input_path); 

  file_log->manifest_->Dump();

  if (index > -1) {
    //for (int i = file_log->manifest_->meta_.entry_end; i > 0; i--) {
      floyd::raft::Entry entry = file_log->GetEntry(index);
      std::string text_format;
      google::protobuf::TextFormat::PrintToString(entry, &text_format);
      printf ("%s------------------------------------\n", text_format.c_str());
   // }
    return 0;
  }


  std::vector<std::string> files;
  int ret = slash::GetChildren(input_path, files);
  if (ret != 0) {
    fprintf(stderr, "open path %s, %s", input_path.c_str(), strerror(ret));
    return -1;
  }

  const std::string kManifest = "manifest";
  const std::string kLog = "floyd.log";

  int tmp = 0;
  sort(files.begin(), files.end());
  for (size_t i = 0; i < files.size(); i++) {
    if ((!file.empty() && files[i] == file) || (file.empty() && files[i].find(kLog) != std::string::npos)) {
      if (tmp < cnt) {
        file_log->ReplaySingleFile(db, input_path + "/" + files[i]);
        tmp++;
      } else {
        break;
      }
    }
  }

  return 0;
}
