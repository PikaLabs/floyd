#include <stdio.h>
#include <iostream>
#include <string>

#include <unistd.h>
#include "slash_status.h"

#include "floyd_client.h"

using namespace std;

//struct option const long_options[] = {
//  {"servers", required_argument, NULL, 's'},
//  {NULL, 0, NULL, 0}, };

//const char* short_options = "s:";

int main(int argc, char* argv[]) {
  floyd::client::Option option;

  option.ParseFromArgs(argc, argv);

  floyd::client::Cluster cluster(option);

  int cnt = 1000;
  if (argc == 4) {
    char *end;
    cnt = strtol(argv[3], &end, 10);
  }
  int start = 0;
  if (argc == 5) {
    char *end;
    start = strtol(argv[3], &end, 10);
    cnt = strtol(argv[4], &end, 10);
  }
  printf ("start=%d cnt=%d\n", start, cnt);
  sleep(3);

  for (int i = 0; i < cnt; i++) {
    printf ("\n=====Test Write==========\n");

    std::string key = "test_key" + std::to_string(i);
    std::string value = "test_value" + std::to_string(i);

    slash::Status result = cluster.Write(key, value);
    if (result.ok()) {
      printf ("Write ok\n");
    } else {
      printf ("Write failed, %s\n", result.ToString().c_str());
    }

    printf ("\n=====Test Read==========\n");
    result = cluster.Read(key, &value);
    if (result.ok()) {
      printf ("read ok, value is %s\n", value.c_str());
    } else {
      printf ("Read failed, %s\n", result.ToString().c_str());
    }

    printf ("\n=====Test ServerStatus==========\n");
    result = cluster.GetStatus(&value);
    if (result.ok()) {
      printf ("GetStatus ok, msg is\n%s", value.c_str());
    } else {
      printf ("GetStatus failed, %s\n", result.ToString().c_str());
    }
  }

  cout << "success" << endl;
  return 0;
}
