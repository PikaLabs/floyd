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
  Options op("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4311, "./data1/");
  op.Dump();

  Floyd *f1, *f2, *f3, *f4, *f5;

  slash::Status s;
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

  int cnt = 100;
  std::string holder = "baotiao-holder";
  while (cnt--) {
    s = f1->TryLock("baotiao-key", holder, 1000);
    printf("TryLock status %s holder %s\n", s.ToString().c_str(), holder.c_str());
    s = f1->UnLock("baotiao-key", holder);
    printf("TryUnLock status %s holder %s\n", s.ToString().c_str(), holder.c_str());
  }

  getchar();
  delete f2;
  delete f3;
  delete f4;
  delete f5;
  delete f1;
  return 0;
}
