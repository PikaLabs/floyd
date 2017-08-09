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
  int cnt = 100;
  uint64_t st = NowMicros(), ed;

  sleep(10);
  while (1) {
    if (f1->HasLeader()) {
      break;
    }
    printf("electing leader... sleep 2s\n");
    sleep(2);
  }

  while (cnt--) {
    std::string mystr[100100];
    for (int i = 0; i < 100000; i++) {
      mystr[i] = slash::RandomString(10);
    }
    f1->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    st = NowMicros();
    for (int j = 0; j < 100000; j++) {
      f1->Write(mystr[j], mystr[j]);
      // f1->Write("zz", "zz");
    }
    ed = NowMicros();
    printf("write 100000 cost time microsecond(us) %ld, qps %llu\n", ed - st, 100000 * 1000000LL / (ed - st));
  }

  delete f1;

  cnt = 5;
  while (cnt--) {
    f2->GetServerStatus(&msg);
    for (int j = 0; j < 100; j++) {
      f2->Write("zz2" + char(j), "value2" + char(j));
    }
    printf("%s\n", msg.c_str());
  }

  s = Floyd::Open(op, &f1);
  if (!s.ok()) {
    printf("floyd reoptn failed\n");
  }
  cnt = 5;
  while (cnt--) {
    f2->GetServerStatus(&msg);
    for (int j = 0; j < 100; j++) {
      f1->Write("zz3" + char(j), "value3" + char(j));
    }
    printf("%s\n", msg.c_str());
  }

  getchar();
  delete f2;
  delete f3;
  delete f4;
  delete f5;
  delete f1;
  return 0;
}
