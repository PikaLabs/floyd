#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <iostream>
#include <sstream>
#include <signal.h>

#include "floyd/include/floyd_server.h"

#include "slash/include/slash_status.h"

void Usage();
const struct option long_options[] = {
  {"servers", required_argument, NULL, 's'},
  {"local_ip", required_argument, NULL, 'i'},
  {"local_port", required_argument, NULL, 'p'},
  {"sdk_port", required_argument, NULL, 'P'},
  {"data_path", required_argument, NULL, 'd'},
  {"log_path", required_argument, NULL, 'l'},
  {NULL, 0, NULL, 0}, };

const char* short_options = "s:i:p:d:l:";

floyd::FloydServer *fs;

void IntSigHandle(int sig) {
  printf ("Catch Signal %d, cleanup...\n", sig);
  fs->server_mutex.Unlock();
}

void SignalSetup() {
  signal(SIGPIPE, SIG_IGN);
  if (signal(SIGINT, &IntSigHandle) == SIG_ERR) {
    printf ("Catch SignalInt error\n");
  }
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char** argv) {
  if (argc < 12) {
    printf ("Usage:\n"
            " ./main --servers ip1:port1,ip2:port2 --local_ip ip --local_port port\n"
            "     --sdk_port portx --data_path data_path --log_path log_path\n");
    exit(0);
  }

  floyd::Options options;

  int ch, longindex;
  int server_port;

  while ((ch = getopt_long(argc, argv, short_options, long_options,
                           &longindex)) >= 0) {
    switch (ch) {
      case 's':
        options.SetMembers(std::string(optarg));
        break;
      case 'i':
        options.local_ip = optarg;
        break;
      case 'p':
        options.local_port = atoi(optarg);
        break;
      case 'P':
        server_port = atoi(optarg);
        break;
      case 'd':
        options.path = optarg;
        break;
      default:
        break;
    }
  }

  options.Dump();

  SignalSetup();

  fs = new floyd::FloydServer(server_port, options);
  slash::Status s = fs->Start();
  if (!s.ok()) {
    printf("Start Floyd Server error\n");
    return -1;
  }

  printf ("Will Stop FloydServer...\n");
  delete fs;

  sleep(1);
  return 0;
}
