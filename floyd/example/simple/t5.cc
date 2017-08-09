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
  printf("testing single mode floyd, including starting a node and writing data\n");
  Options op("127.0.0.1:8901", "127.0.0.1", 8901, "./data1/");
  op.Dump();

  Floyd *f1;

  op.single_mode = true;
  slash::Status s;
  s = Floyd::Open(op, &f1);
  printf("%s\n", s.ToString().c_str());

  std::string msg;
  int cnt = 10;
  while (cnt--) {
    f1->GetServerStatus(&msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }
  uint64_t st, ed;
  std::string mystr[100100];
  for (int i = 0; i < 100000; i++) {
    mystr[i] = slash::RandomString(10);
  }
  f1->GetServerStatus(&msg);
  printf("%s\n", msg.c_str());
  st = NowMicros();
  for (int j = 0; j < 100000; j++) {
    slash::Status ws = f1->Write(mystr[j], mystr[j]);
    if (!ws.ok()) {
      printf("floyd write error\n");
    }
    // f1->Write("zz", "zz");
  }
  ed = NowMicros();
  printf("write 100000 cost time microsecond(us) %ld, qps %llu\n", ed - st, 100000 * 1000000LL / (ed - st));
  getchar();
  delete f1;
 
  return 0;
}
