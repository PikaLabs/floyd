#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>

#include <iostream>
#include <string>

#include "floyd/include/floyd.h"
#include "slash/include/testutil.h"

using namespace floyd;
uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

int main()
{
  Options op("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8901, "./data1/");
  op.Dump();

  Floyd *f1, *f2, *f3, *f4, *f5;

  slash::Status s;
  s = Floyd::Open(op, &f1);
  printf("%s\n", s.ToString().c_str());

  usleep(100000);
  Options op2("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8902, "./data2/");
  s = Floyd::Open(op2, &f2);
  printf("%s\n", s.ToString().c_str());

  usleep(200000);
  Options op3("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8903, "./data3/");
  s = Floyd::Open(op3, &f3);
  printf("%s\n", s.ToString().c_str());

  usleep(300000);
  Options op4("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8904, "./data4/");
  s = Floyd::Open(op4, &f4);
  printf("%s\n", s.ToString().c_str());

  usleep(400000);
  Options op5("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8905, "./data5/");
  s = Floyd::Open(op5, &f5);
  printf("%s\n", s.ToString().c_str());

  std::string msg;
  int i = 8;
  while (i--) {
    f1->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }

  printf("delete node2, node3 to show 3 nodes status\n");
  delete f2;
  delete f3;
  i = 8;
  while (i--) {
    f1->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }

  printf("restart node2, node3 and stop node4, show 4 nodes status\n");
  s = Floyd::Open(op2, &f2);
  s = Floyd::Open(op3, &f3);
  delete f4;
  i = 8;
  while (i--) {
    f1->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }

  printf("reopen node4, delete node1, node5 show 3 nodes status\n");
  s = Floyd::Open(op4, &f4);
  delete f1;
  delete f5;
  i = 8;
  while (i--) {
    f3->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }

  printf("delete node2, now only two nodes are alive\n");
  delete f2;
  i = 8;
  while (i--) {
    f3->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }

  printf("reopen node1, node2, node5, the cluster recover now\n");
  s = Floyd::Open(op, &f1);
  s = Floyd::Open(op2, &f2);
  s = Floyd::Open(op5, &f5);
  i = 8;
  while (i--) {
    f1->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }

  getchar();
  delete f2;
  delete f3;
  delete f4;
  delete f5;
  delete f1;
 
  return 0;
}
