
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
  // Options op("127.0.0.1:8901", "127.0.0.1", 8901, "./data1/");
  Options op("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903", "127.0.0.1", 8901, "./data1/");
  Floyd *f1, *f2, *f3, *f4, *f5;
  op.Dump();

  slash::Status s = Floyd::Open(op, &f1);
  printf("%s\n", s.ToString().c_str());

  Options op2("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903", "127.0.0.1", 8902, "./data2/");
  s = Floyd::Open(op2, &f2);
  printf("%s\n", s.ToString().c_str());

  Options op3("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903", "127.0.0.1", 8903, "./data3/");
  s = Floyd::Open(op3, &f3);
  printf("%s\n", s.ToString().c_str());

  // Options op4("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8904, "./data4/");
  // s = Floyd::Open(op4, &f4);
  // printf("%s\n", s.ToString().c_str());

  // Options op5("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8905, "./data5/");
  // s = Floyd::Open(op5, &f5);
  // printf("%s\n", s.ToString().c_str());
  std::string msg;
  int i = 10;
  uint64_t st = NowMicros(), ed;
  /*
   * std::string mystr[10010];
   * for (int i = 0; i < 10000; i++) {
   *   mystr[i] = slash::RandomString(10);
   * }
   */
  sleep(10);
  while (i--) {
    sleep(2);
    f1->GetServerStatus(msg);
    printf("%s\n", msg.c_str());
    // st = NowMicros();
    for (int j = 0; j < 1; j++) {
      // f1->Write(mystr[j], mystr[j]);
      f1->Write("zz", "zz");
    }
    // ed = NowMicros();
    // printf("write 10000 cost time microsecond(us) %lld, qps %lld\n", ed - st, 10000 * 1000000LL / (ed - st));
  }

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
