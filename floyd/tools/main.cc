#include <iostream>
#include "file_log.h"

using namespace std;
using namespace floyd::raft;

void Usage() {
  printf ("Usage:\n"
          "  ./parse -d ./log/path/ -c cnt          ----  scan the first cnt files \n"
          "  ./parse -d ./log/path/ -f pecific_file ----  scan single file\n");
}

int main(int argc, char* argv[]) {
  if (argc != 3 && argc != 5) {
    Usage();
    exit(-1);
  }

  std::string path;
  std::string file;
  int cnt = 1;

  char ch;
  while (-1 != (ch = getopt(argc, argv, "d:f:c:"))) {
    switch (ch) {
      case 'd':
        path = optarg;
        break;
      case 'c':
        char *pend;
        cnt = std::strtol(optarg, &pend, 10);
        break;
      case 'f':
        file = optarg;
        break;
      default:
        Usage();
        exit(-1);
    }
  }


  printf ("log_path : %s\n", path.c_str());
  FileLog* file_log = new FileLog(path); 

  file_log->manifest_->Dump();


  std::vector<std::string> files;
  int ret = slash::GetChildren(path, files);
  if (ret != 0) {
    fprintf(stderr, "open path %s, %s", path.c_str(), strerror(ret));
    return -1;
  }

  const std::string kManifest = "manifest";
  const std::string kLog = "floyd.log";

  int tmp = 0;
  sort(files.begin(), files.end());
  for (size_t i = 0; i < files.size(); i++) {
    if ((!file.empty() && files[i] == file) || (file.empty() && files[i].find(kLog) != std::string::npos)) {
      if (tmp < cnt) {
        file_log->DumpSingleFile(path + "/" + files[i]);
        tmp++;
      } else {
        break;
      }
    }
  }

  return 0;
}
