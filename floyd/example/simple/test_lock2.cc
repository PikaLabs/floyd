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

  std::string key_name = "baotiao-key";
  // 1. lock by f1
  std::string holder = "baotiao-holder";
  s = f1->TryLock(key_name, holder, 1000);
  printf("1. TryLock status %s name %s holder %s\n", s.ToString().c_str(), key_name.c_str(), holder.c_str());

  // 2. delete f1 node
  delete f1;

  // 3. TryLock by other node
  // here the TryLock should be failed
  while (1) {
    if (f2->HasLeader()) {
      break;
    }
    printf("electing leader... sleep 2s\n");
    sleep(2);
  }
  std::string holder2 = "baotiao-holder2";
  s = f2->TryLock(key_name, holder2, 1000);
  printf("3. TryLock should be failed, since the lock hasn't been unlock\n");
  printf("3. TryLock status %s name %s holder %s\n", s.ToString().c_str(), key_name.c_str(), holder2.c_str());

  // 4. Unlock by other node
  // here the Unlock should be success
  s = f2->UnLock(key_name, holder);
  printf("4. Unlock by other node, here the Unlock should be success\n");
  printf("4. UnLock status %s name %s holder %s\n", s.ToString().c_str(), key_name.c_str(), holder.c_str());

  // 5. TryLock by other node again
  // here the TryLock should be success
  s = f2->TryLock(key_name, holder2, 1000);
  printf("5. TryLock should be success\n");
  printf("5. TryLock status %s name %s holder %s\n", s.ToString().c_str(), key_name.c_str(), holder2.c_str());

  // 6. delete more node
  delete f2;
  delete f3;
  printf("6. Delete more nodes, test lock when the cluster is error\n");
  s = f4->UnLock(key_name, holder2);
  printf("6. TryLock status %s name %s holder %s\n", s.ToString().c_str(), key_name.c_str(), holder2.c_str());


  // 7. test node with cluster recover
  s = Floyd::Open(op, &f1);
  s = Floyd::Open(op2, &f2);
  s = Floyd::Open(op3, &f3);
  while (1) {
    if (f1->HasLeader()) {
      break;
    }
    printf("electing leader... sleep 2s\n");
    sleep(2);
  }
  s = f2->TryLock(key_name, holder, 1000);
  printf("7. test node with cluster recover\n");
  printf("7. UnLock status %s name %s holder %s\n", s.ToString().c_str(), key_name.c_str(), holder2.c_str());

  // 8. test lock expired
  
  printf("8. the lock is holded by %s, when time expired the lock should be success\n", holder.c_str());
  sleep(10);
  s = f2->TryLock(key_name, holder, 1000);
  printf("8. sleep 10s, the trylock should be success\n");
  printf("8. TryLock status %s name %s holder %s\n", s.ToString().c_str(), key_name.c_str(), holder2.c_str());

  
  getchar();
  delete f2;
  delete f3;
  delete f4;
  delete f5;
  return 0;
}
