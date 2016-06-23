#include <stdio.h>
#include <iostream>
#include <string>

#include "client.h"

using namespace std;

//struct option const long_options[] = {
//  {"servers", required_argument, NULL, 's'},
//  {NULL, 0, NULL, 0}, };

//const char* short_options = "s:";

int main(int argc, char* argv[]) {
  floyd::client::Option option;

  option.ParseFromArgs(argc, argv);

  floyd::client::Cluster cluster(option);

  printf ("\n=====Test Write==========\n");

  std::string key = "test_key";
  std::string value = "test_value";
  
  floyd::Status result = cluster.Write(key, value);
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

  cout << "success" << endl;
  return 0;
}
