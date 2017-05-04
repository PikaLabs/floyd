#include <iostream>
#include <google/protobuf/text_format.h>
#include "file_log.h"

using namespace std;
using namespace floyd;

void Usage() {
  printf ("Usage:\n"
          "  ./parse -d ./log/path/ -c cnt          ----  scan the first cnt files \n"
          "  ./parse -d ./log/path/ -f pecific_file ----  scan single file\n"
          "  ./parse -d ./log/path/ -r index        ----  read specific index entry\n");
}

int main(int argc, char* argv[]) {
  if (argc != 3 && argc != 5) {
    Usage();
    exit(-1);
  }

  std::string path;
  std::string file;
  int cnt = 1;
  int index = 0;

  char ch;
  while (-1 != (ch = getopt(argc, argv, "d:f:c:r:"))) {
    switch (ch) {
      case 'd':
        path = optarg;
        break;
      case 'c': {
        char *pend;
        cnt = std::strtol(optarg, &pend, 10);
        break;
      }
      case 'f':
        file = optarg;
        break;
      case 'r': {
        char *pend;
        index = std::strtol(optarg, &pend, 10);
        break;
      }
      default:
        Usage();
        exit(-1);
    }
  }


  printf ("log_path : %s\n", path.c_str());
  FileLog* file_log = new FileLog(path); 

  file_log->manifest_->Dump();

  if (index > 0) {
    //for (int i = file_log->manifest_->meta_.entry_end; i > 0; i--) {
      Entry entry;
      file_log->GetEntry(index, &entry);
      std::string text_format;
      google::protobuf::TextFormat::PrintToString(entry, &text_format);
      printf ("%s------------------------------------\n", text_format.c_str());
   // }
    return 0;
  }


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
