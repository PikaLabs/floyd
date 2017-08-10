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

std::string keystr[1001000];
std::string valstr[1001000];
int val_size = 128;
int item_num = 1000;

int main(int argc, char *argv[])
{
  if (argc > 1) {
    val_size = atoi(argv[1]);
  }

  printf("test write 3 node and then join the other 2 node case, key size %d\n", val_size);

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
  int cnt = 1;
  uint64_t st = NowMicros(), ed;
  for (int i = 0; i < item_num; i++) {
    keystr[i] = slash::RandomString(32);
  }

  while (1) {
    if (f1->HasLeader()) {
      break;
    }
    printf("electing leader... sleep 2s\n");
    sleep(2);
  }

  while (cnt--) {
    for (int i = 0; i < item_num; i++) {
      valstr[i] = slash::RandomString(10);
    }
    f1->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    st = NowMicros();
    for (int j = 0; j < item_num; j++) {
      slash::Status s1 = f1->Write(keystr[j], valstr[j]);
      if (!s1.ok()) {
        printf("write key error in 5 node, key is %s\n", keystr[j].c_str());
      }
    }
    ed = NowMicros();
    printf("write 100000 cost time microsecond(us) %ld, qps %llu\n", ed - st, item_num * 1000000LL / (ed - st));
  }

  delete f1;
  delete f5;

  sleep(10);
  while (1) {
    if (f2->HasLeader()) {
      break;
    }
    printf("electing leader... sleep 2s\n");
    sleep(2);
  }
  cnt = 1;
  while (cnt--) {
    for (int i = 0; i < item_num; i++) {
      keystr[i] = slash::RandomString(32);
    }
    for (int i = 0; i < item_num; i++) {
      valstr[i] = slash::RandomString(10);
    }
    f2->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    st = NowMicros();
    for (int j = 0; j < item_num; j++) {
      slash::Status s1 = f2->Write(keystr[j], valstr[j]);
      if (s.ok()) {
        printf("write key success number %d %s %s\n", j, keystr[j].c_str(), valstr[j].c_str());
      } else {
        printf("write error\n");
      }
    }
    ed = NowMicros();
    printf("write after delete two nodes cost time microsecond(us) %ld, qps %llu\n", ed - st, item_num * 1000000LL / (ed - st));
  }

  s = Floyd::Open(op, &f1);
  s = Floyd::Open(op5, &f5);

  cnt = 10;
  while (cnt--) {
    f2->GetServerStatus(&msg);
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
