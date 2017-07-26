// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>

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

Floyd *f1;
std::string mystr[100100];
void *fun(void *arg) {
  int i = 1;
  while (i--) {
    for (int j = 0; j < 100000; j++) {
      f1->Write(mystr[j], mystr[j]);
    }
  }
}

int main()
{
  // Options op("127.0.0.1:8907", "127.0.0.1", 8907, "./data1/");
  Options op("127.0.0.1:8907,127.0.0.1:8908,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8907, "./data1/");
  Floyd *f2, *f3, *f4, *f5;
  op.Dump();

  slash::Status s = Floyd::Open(op, &f1);
  printf("%s\n", s.ToString().c_str());

  Options op2("127.0.0.1:8907,127.0.0.1:8908,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8908, "./data2/");
  s = Floyd::Open(op2, &f2);
  printf("%s\n", s.ToString().c_str());

  Options op3("127.0.0.1:8907,127.0.0.1:8908,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8903, "./data3/");
  s = Floyd::Open(op3, &f3);
  printf("%s\n", s.ToString().c_str());

  Options op4("127.0.0.1:8907,127.0.0.1:8908,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8904, "./data4/");
  s = Floyd::Open(op4, &f4);
  printf("%s\n", s.ToString().c_str());

  Options op5("127.0.0.1:8907,127.0.0.1:8908,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8905, "./data5/");
  s = Floyd::Open(op5, &f5);
  printf("%s\n", s.ToString().c_str());

  std::string msg;
  int i = 10;
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

  pthread_t pid[24];
  st = NowMicros();
  for (int i = 0; i < 8; i++) {
    pthread_create(&pid[i], NULL, fun, NULL);
  } 
  for (int i = 0; i < 8; i++) {
    pthread_join(pid[i], NULL);
  }
  ed = NowMicros();
  printf("write 100000 cost time microsecond(us) %ld, qps %llu\n", ed - st, 100000 * 8 * 1000000LL / (ed - st));


  getchar();
  delete f2;
  delete f3;
  delete f4;
  delete f5;
  delete f1;
  return 0;
}
