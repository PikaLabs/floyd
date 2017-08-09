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

  bool is_writed = false;
  std::string msg;
  while (1) {
    f1->GetServerStatus(&msg);
    if (is_writed == false) {
      // writing one record to update the meta data
      slash::Status s = f1->Write("lastdata", "lastdata");
      if (s.ok()) {
        is_writed = true;
        printf("write lastdata success\n");
      }
    }
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
