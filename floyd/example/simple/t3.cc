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
std::string mystr[100100];

int main()
{
  Options op("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8901, "./data1/");
  op.Dump();

  Floyd *f1, *f2, *f3, *f4, *f5;

  slash::Status s;
  s = Floyd::Open(op, &f1);
  printf("%s\n", s.ToString().c_str());

  Options op2("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8902, "./data2/");
  s = Floyd::Open(op2, &f2);
  printf("%s\n", s.ToString().c_str());

  Options op3("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8903, "./data3/");
  s = Floyd::Open(op3, &f3);
  printf("%s\n", s.ToString().c_str());

  Options op4("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8904, "./data4/");
  s = Floyd::Open(op4, &f4);
  printf("%s\n", s.ToString().c_str());

  Options op5("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8905, "./data5/");
  s = Floyd::Open(op5, &f5);
  printf("%s\n", s.ToString().c_str());

  std::string msg;
  int i = 100;
  uint64_t st = NowMicros(), ed;
  for (int i = 0; i < 100000; i++) {
    mystr[i] = slash::RandomString(10);
  }

  while (1) {
    if (f1->HasLeader()) {
      break;
    }
    sleep(2);
  }

  // ran at the begining
  printf("run 5 times, every time write 100 item. at the beginning state\n");
  i = 5;
  std::string val;
  while (i--) {
    f3->GetServerStatus(&msg);
    for (int j = 0; j < 100; j++) {
      f3->Write(mystr[j], mystr[j]);
      s = f3->Read(mystr[j], &val);
      printf("status %s val %s\n", s.ToString().c_str(), val.c_str());
    }
    printf("%s\n", msg.c_str());
  }

  // sleep(1);


  getchar();
  return 0;
}
