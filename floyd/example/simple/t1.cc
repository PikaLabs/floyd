
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

Floyd *f1;
std::string mystr[10010];

void *fun(void *arg) {
  int i = 1;
  while (i--) {
    // sleep(2);
    for (int j = 0; j < 10000; j++) {
      f1->Write(mystr[j], mystr[j]);
    }
  }

}

int main()
{
  // Options op("127.0.0.1:8901", "127.0.0.1", 8901, "./data1/");
  Options op("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8901, "./data1/");
  Floyd *f2, *f3, *f4, *f5;
  op.Dump();

  slash::Status s = Floyd::Open(op, &f1);
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
  for (int i = 0; i < 10000; i++) {
    mystr[i] = slash::RandomString(10);
  }
  sleep(20);

  pthread_t pid[10];
  st = NowMicros();
  for (int i = 0; i < 10; i++) {
    pthread_create(&pid[i], NULL, fun, NULL);
  } 
  for (int i = 0; i < 10; i++) {
    pthread_join(pid[i], NULL);
  }
  ed = NowMicros();
  printf("write 10000 cost time microsecond(us) %lld, qps %lld\n", ed - st, 10000 * 10 * 1000000LL / (ed - st));

/*
 *   delete f1;
 * 
 *   i = 5;
 *   while (i--) {
 *     sleep(3);
 *     f2->GetServerStatus(msg);
 *     for (int j = 0; j < 100; j++) {
 *       f2->Write("zz2" + char(j), "value2" + char(j));
 *     }
 *     printf("%s\n", msg.c_str());
 *   }
 * 
 *   s = Floyd::Open(op, &f1);
 *   i = 5;
 *   while (i--) {
 *     sleep(3);
 *     f2->GetServerStatus(msg);
 *     for (int j = 0; j < 100; j++) {
 *       f1->Write("zz3" + char(j), "value3" + char(j));
 *     }
 *     printf("%s\n", msg.c_str());
 *   }
 */

  getchar();
  return 0;
}
