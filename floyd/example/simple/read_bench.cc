// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <signal.h>

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

Floyd *f1, *f2, *f3, *f4, *f5;
std::string keystr[1001000];
std::string valstr[1001000];
int val_size = 10;
int thread_num = 32;
int item_num = 100000;

void *fun(void *arg) {
  int i = 1;
  Floyd *p;
  if (f1->IsLeader()) {
    p = f1;
  } else if (f2->IsLeader()) {
    p = f2;
  } else if (f3->IsLeader()) {
    p = f3;
  } else if (f4->IsLeader()) {
    p = f4;
  } else {
    p = f5;
  }
  std::string val;
  while (i--) {
    for (int j = 0; j < item_num; j++) {
      p->Read(keystr[j], &val);
    }
  }
}

int main(int argc, char * argv[])
{
  if (argc > 1) {
    thread_num = atoi(argv[1]);
  }
  if (argc > 2) {
    val_size = atoi(argv[2]);
  }
  if (argc > 3) {
    item_num = atoi(argv[3]);
  }

  printf("multi threads test to get performance thread num %d key size %d item number %d\n", thread_num, val_size, item_num);

  Options op1("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8901, "./data1/");
  slash::Status s = Floyd::Open(op1, &f1);
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
  int i = 10;
  uint64_t st = NowMicros(), ed;
  for (int i = 0; i < item_num; i++) {
    keystr[i] = slash::RandomString(32);
  }
  for (int i = 0; i < item_num; i++) {
    valstr[i] = slash::RandomString(val_size);
  }
  while (1) {
    if (f1->HasLeader()) {
      f1->GetServerStatus(&msg);
      printf("%s\n", msg.c_str());
      break;
    }
    printf("electing leader... sleep 2s\n");
    sleep(2);
  }
  for (int i = 0; i < item_num; i++) {
    f1->Write(keystr[i], valstr[i]);
  }

  pthread_t pid[24];
  st = NowMicros();
  for (int i = 0; i < thread_num; i++) {
    pthread_create(&pid[i], NULL, fun, NULL);
  } 
  for (int i = 0; i < thread_num; i++) {
    pthread_join(pid[i], NULL);
  }
  ed = NowMicros();
  printf("read_bench reading %d datas cost time microsecond(us) %ld, qps %llu\n", 
      item_num * thread_num, ed - st, item_num * thread_num * 1000000LL / (ed - st));

  getchar();
  delete f2;
  delete f3;
  delete f4;
  delete f5;
  delete f1;
  return 0;
}
