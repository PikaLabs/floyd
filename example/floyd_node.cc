#include "floyd_server.h"
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <iostream>
#include <sstream>

static struct option const long_options[] = {
    {"server_ip", required_argument, NULL, 'n'},
    {"server_port", required_argument, NULL, 'm'},
    {"seed_ip", required_argument, NULL, 'I'},
    {"seed_port", required_argument, NULL, 'P'},
    {"local_ip", required_argument, NULL, 'i'},
    {"local_port", required_argument, NULL, 'p'},
    {"data_path", required_argument, NULL, 'd'},
    {"log_path", required_argument, NULL, 'l'},
    {NULL, 0, NULL, 0}, };

static const char* short_options = "n:m:I:P:i:p:d:l:";
using namespace std;
int main(int argc, char** argv) {
  int ch, longindex;
  floyd::Options options;
  string server_ip;
  int server_port;
  while ((ch = getopt_long(argc, argv, short_options, long_options,
                           &longindex)) >= 0) {
    switch (ch) {
      case 'n':
        server_ip = optarg;
        break;
      case 'm':
        server_port = atoi(optarg);
        break;
      case 'I':
        options.seed_ip = optarg;
        break;
      case 'P':
        options.seed_port = atoi(optarg);
        break;
      case 'i':
        options.local_ip = optarg;
        break;
      case 'p':
        options.local_port = atoi(optarg);
        break;
      case 'd':
        options.data_path = optarg;
        break;
      case 'l':
        options.log_path = optarg;
        break;
      default:
        break;
    }
  }

  options.storage_type = "leveldb";

  // options.log_type = "SimpleFileLog";
  options.log_type = "FileLog";
  signal(SIGPIPE, SIG_IGN);
  FloydServer server(server_ip, server_port, options);
  server.Start();
  cout << "success" << endl;
  sleep(1000);
  return 0;
}
