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

  usleep(100000);
  Options op2("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8902, "./data2/");
  s = Floyd::Open(op2, &f2);
  printf("%s\n", s.ToString().c_str());

  usleep(200000);
  Options op3("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8903, "./data3/");
  s = Floyd::Open(op3, &f3);
  printf("%s\n", s.ToString().c_str());

  usleep(300000);
  Options op4("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8904, "./data4/");
  s = Floyd::Open(op4, &f4);
  printf("%s\n", s.ToString().c_str());

  usleep(400000);
  Options op5("127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905", "127.0.0.1", 8905, "./data5/");
  s = Floyd::Open(op5, &f5);
  printf("%s\n", s.ToString().c_str());

  std::string msg;
  int i = 8;
  while (i--) {
    f1->GetServerStatus(msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }

  delete f2;
  delete f3;
  while (1) {
    f1->GetServerStatus(msg);
    printf("%s\n", msg.c_str());
    sleep(2);
  }
/*
 *   std::string msg;
 *   int i = 100;
 *   uint64_t st = NowMicros(), ed;
 *   std::string mystr[10010];
 *   for (int i = 0; i < 10000; i++) {
 *     mystr[i] = slash::RandomString(10);
 *   }
 * 
 *   sleep(10);
 *   while (1) {
 *     if (f1->HasLeader()) {
 *       break;
 *     }
 *     sleep(2);
 *     printf("electing leader, waitting...\n");
 *   }
 * 
 *   // ran at the begining
 *   printf("run 5 times, every time write 100 item. at the beginning state\n");
 *   i = 5;
 *   while (i--) {
 *     f2->GetServerStatus(msg);
 *     for (int j = 0; j < 100; j++) {
 *       f2->Write(mystr[j], mystr[j]);
 *     }
 *     printf("%s\n", msg.c_str());
 *   }
 * 
 * 
 *   // delete two node
 *   delete f1;
 *   delete f3;
 * 
 *   while (1) {
 *     if (f2->HasLeader()) {
 *       break;
 *     }
 *     sleep(2);
 *     printf("electing leader, waitting...\n");
 *   }
 *   // ran with three node
 *   printf("delete two node, run 5 times, every time write 100 item with three node\n");
 *   i = 5;
 *   while (i--) {
 *     f2->GetServerStatus(msg);
 *     for (int j = 0; j < 100; j++) {
 *       f2->Write(mystr[j], mystr[j]);
 *     }
 *     printf("%s\n", msg.c_str());
 *   }
 * 
 *   s = Floyd::Open(op, &f1);
 *   if (!s.ok()) {
 *     printf("floyd reoptn failed\n");
 *   }
 *   s = Floyd::Open(op3, &f3);
 *   if (!s.ok()) {
 *     printf("floyd reoptn failed\n");
 *   }
 * 
 *   while (1) {
 *     if (f1->HasLeader()) {
 *       break;
 *     }
 *     sleep(2);
 *     printf("electing leader, waitting...\n");
 *   }
 *   // ran with node recovery
 *   printf("recovery the two node, run 5 time, every time write 100 item\n");
 *   i = 5;
 *   while (i--) {
 *     f2->GetServerStatus(msg);
 *     for (int j = 0; j < 100; j++) {
 *       f1->Write(mystr[j], mystr[j]);
 *     }
 *     printf("%s\n", msg.c_str());
 *   }
 * 
 *   // at last, we will have 300 log, 100 db
 */
    getchar();
    delete f2;
    delete f3;
    delete f4;
    delete f5;
    delete f1;
 
  return 0;
}
