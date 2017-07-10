#include <stdio.h>
#include <iostream>
#include <string>

#include <unistd.h>
#include <getopt.h>
#include "logger.h"

#include "slash/include/slash_status.h"

#include "kv_cli.h"

using namespace std;


static struct option const long_options[] = {
  {"server", required_argument, NULL, 's'},
  {"cmd", required_argument, NULL, 't'},
  {"begin", required_argument, NULL, 'b'},
  {"end", required_argument, NULL, 'e'},
  {NULL, 0, NULL, 0} };

int main(int argc, char* argv[]) {
  if (argc < 2) {
    fprintf (stderr, "Usage:\n"
            "  ./client --server ip:port\n"
            "           --cmd [read | write | delete | status | debug_on | debug_off]\n"
            "           --begin id0 --end id1\n");
    exit(-1);
  }

  int cnt = 1000;
  int begin = 0;
  std::string server_str, cmd;
  int opt, optindex;
  while ((opt = getopt_long(argc, argv, "s:", long_options, &optindex)) != -1) {
    switch (opt) {
      case 's':
        server_str = optarg;
        break;
      case 't':
        cmd = optarg;
        break;
      case 'b': {
        char *end;
        begin = strtol(optarg, &end, 10);
        break;
      }
      case 'e': {
        char *end;
        cnt = strtol(optarg, &end, 10);
        break;
      }
      default:
        break;
    }
  }
  floyd::client::Option option(server_str);
  floyd::client::Cluster cluster(option);

  printf ("Will connect(%s) with cmd(%s), begin=%d cnt=%d\n", server_str.c_str(), cmd.c_str(), begin, cnt);
  sleep(1);

  for (int i = begin; i < cnt; i++) {
    std::string key = "test_key" + std::to_string(i);
    std::string value = "test_value" + std::to_string(i);
    
    slash::Status result;

    if (cmd.empty() || cmd == "write") {
      //fprintf (stderr, "\n=====Test Write==========\n");
      result = cluster.Write(key, value);
      if (result.ok()) {
        fprintf (stderr, "Write key(%s) ok\n", key.c_str());
      } else {
        fprintf (stderr, "Write key(%s) failed, %s\n", key.c_str(), result.ToString().c_str());
      }
    }

    if (cmd.empty() || cmd == "dirtywrite") {
      //fprintf (stderr, "\n=====Test DirtyWrite==========\n");
      result = cluster.DirtyWrite(key, value);
      if (result.ok()) {
        fprintf (stderr, "DirtyWrite key(%s) ok\n", key.c_str());
      } else {
        fprintf (stderr, "DirtyWrite key(%s) failed, %s\n", key.c_str(), result.ToString().c_str());
      }
    }

    if (cmd.empty() || cmd == "read") {
      value = "";
      //fprintf (stderr, "\n=====Test Read==========\n");
      result = cluster.Read(key, &value);
      if (result.ok()) {
        fprintf (stderr, "Read key(%s) ok, value is %s\n", key.c_str(), value.c_str());
      } else {
        fprintf (stderr, "Read key(%s) failed, %s\n", key.c_str(), result.ToString().c_str());
      }
    }
    
    if (cmd.empty() || cmd == "dirtyread") {
      value = "";
      //fprintf (stderr, "\n=====Test DirtyRead==========\n");
      result = cluster.DirtyRead(key, &value);
      if (result.ok()) {
        fprintf (stderr, "DirtyRead key(%s) ok, value is %s\n", key.c_str(), value.c_str());
      } else {
        fprintf (stderr, "DirtyRead key(%s) failed, %s\n", key.c_str(), result.ToString().c_str());
      }
    }

    if (cmd.empty() || cmd == "status") {
      value = "";
      //fprintf (stderr, "\n=====Test ServerStatus==========\n");
      result = cluster.GetStatus(&value);
      if (result.ok()) {
        fprintf (stderr, "GetStatus ok, msg is\n%s\n", value.c_str());
      } else {
        fprintf (stderr, "GetStatus failed, %s\n", result.ToString().c_str());
      }
    }

    if (cmd.empty() || cmd == "delete") {
      //fprintf (stderr, "\n=====Test Delete==========\n");
      result = cluster.Delete(key);
      if (result.ok()) {
        fprintf (stderr, "Delete key(%s) ok\n", key.c_str());
      } else {
        fprintf (stderr, "Delete key(%s) failed, %s\n", key.c_str(), result.ToString().c_str());
      }
    }

    if (cmd == "debug_on") {
      result = cluster.set_log_level(floyd::client::DEBUG_LEVEL);
      if (result.ok()) {
        fprintf (stderr, "set log_level to debug ok\n");
      } else {
        fprintf (stderr, "set log_level to debug failed, %s\n", result.ToString().c_str());
      }
    }
    if (cmd == "debug_off") {
      result = cluster.set_log_level(floyd::client::INFO_LEVEL);
      if (result.ok()) {
        fprintf (stderr, "set log_level to info ok\n");
      } else {
        fprintf (stderr, "set log_level to info failed, %s\n", result.ToString().c_str());
      }
    }
  }

  return 0;
}
