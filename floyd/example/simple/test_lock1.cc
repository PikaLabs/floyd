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

Floyd *f1, *f2, *f3, *f4, *f5;
slash::Status s;

/*
 * in this case, thread1 get the lock first, and then sleep 6 second
 * during this time, thread2 try to get lock every second, it will failed every time.
 * at the 7 second, thread2 may get the lock
 */

void *thread1_fun(void *arg) {
  std::string holder = "thread1_fun";
  while (1) {
    s = f1->TryLock("baotiao-key", holder, 10000);
    printf("thread1 TryLock status %s holder %s\n", s.ToString().c_str(), holder.c_str());
    sleep(6);
    s = f1->UnLock("baotiao-key", holder);
    printf("thread2 TryUnLock status %s holder %s\n", s.ToString().c_str(), holder.c_str());
  }
}

void *thread2_fun(void *arg) {
  std::string holder = "thread2_fun";
  while (1) {
    s = f1->TryLock("baotiao-key", holder, 10000);
    printf("thread2 TryLock status %s holder %s\n", s.ToString().c_str(), holder.c_str());
    s = f1->UnLock("baotiao-key", holder);
    printf("thread2 TryUnLock status %s holder %s\n", s.ToString().c_str(), holder.c_str());
    sleep(1);
  }
}

int main()
{
  Options op("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4311, "./data1/");
  op.Dump();


  s = Floyd::Open(op, &f1);
  printf("%s\n", s.ToString().c_str());

  Options op2("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4312, "./data2/");
  s = Floyd::Open(op2, &f2);
  printf("%s\n", s.ToString().c_str());

  Options op3("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4313, "./data3/");
  s = Floyd::Open(op3, &f3);
  printf("%s\n", s.ToString().c_str());

  Options op4("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4314, "./data4/");
  s = Floyd::Open(op4, &f4);
  printf("%s\n", s.ToString().c_str());

  Options op5("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4315, "./data5/");
  s = Floyd::Open(op5, &f5);
  printf("%s\n", s.ToString().c_str());

  std::string msg;
  uint64_t st = NowMicros(), ed;

  while (1) {
    if (f1->HasLeader()) {
      break;
    }
    printf("electing leader... sleep 2s\n");
    sleep(2);
  }

  pthread_t t1, t2;
  pthread_create(&t1, NULL, thread1_fun, NULL);
  sleep(1);
  pthread_create(&t2, NULL, thread2_fun, NULL);

  pthread_join(t1, NULL);
  pthread_join(t2, NULL);

  getchar();
  delete f2;
  delete f3;
  delete f4;
  delete f5;
  delete f1;
  return 0;
}
