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

bool print_members(Floyd* f) {
  std::set<std::string> nodes;
  Status s = f->GetAllServers(&nodes);
  if (!s.ok()) {
    return false;
  }
  printf("Membership: ");
  for (const std::string& n : nodes) {
    printf(" %s", n.c_str());
  }
  printf("\n");
  return true;
}

int main()
{
  slash::Status s;
  Floyd *f1, *f2, *f3, *f4, *f5;

  Options op("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4311, "./data1/");
  s = Floyd::Open(op, &f1);
  printf("start floyd f1 status %s\n", s.ToString().c_str());

  Options op2("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4312, "./data2/");
  s = Floyd::Open(op2, &f2);
  printf("start floyd f2 status %s\n", s.ToString().c_str());

  Options op3("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4313, "./data3/");
  s = Floyd::Open(op3, &f3);
  printf("start floyd f3 status %s\n", s.ToString().c_str());
  
  Options op4("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4314, "./data4/");
  s = Floyd::Open(op4, &f4);
  printf("start floyd f4 status %s\n", s.ToString().c_str());
  
  Options op5("127.0.0.1:4311,127.0.0.1:4312,127.0.0.1:4313,127.0.0.1:4314,127.0.0.1:4315", "127.0.0.1", 4315, "./data5/");
  s = Floyd::Open(op5, &f5);
  printf("start floyd f5 status %s\n", s.ToString().c_str());


  std::string msg;

  while (1) {
    if (print_members(f1)) {
      break;
    }
    printf("electing leader... sleep 2s\n");
    sleep(2);
  }

  // write some data in the origin cluster

  std::string mystr[100100];
  std::string val;
  for (int i = 0; i < 100; i++) {
    mystr[i] = slash::RandomString(10);
  }
  for (int i = 0; i < 10; i++) {
    f1->Write(mystr[i], mystr[i]);
    s = f1->Read(mystr[i], &val);
    printf("status %s val %s\n", s.ToString().c_str(), val.c_str());
  }

  getchar();
  // then remove one server
  printf("Remove out server4 to cluster\n");
  s = f1->RemoveServer("127.0.0.1:4314");
  delete f4;
  printf("Remove out server4 status %s\n", s.ToString().c_str());

  while (1) {
    if (print_members(f1)) {
      break;
    }
    printf("electing leader after server 4 leave the cluster... sleep 2s\n");
    sleep(2);
  }
  printf("Write 100 pairs key-value after f4 node leave from cluster\n");
  for (int i = 0; i < 10; i++) {
    f1->Write(mystr[i], mystr[i]);
    s = f1->Read(mystr[i], &val);
    printf("status %s val %s\n", s.ToString().c_str(), val.c_str());
  }


  getchar();
  // then remove one more server
  printf("Remove out server5 to cluster\n");
  s = f1->RemoveServer("127.0.0.1:4315");
  delete f5;
  printf("Remove out server5 status %s\n", s.ToString().c_str());
  while (1) {
    if (print_members(f1)) {
      break;
    }
    printf("electing leader after server 5 leave the cluster... sleep 2s\n");
    sleep(2);
  }
  printf("Write 100 pairs key-value after f5 node leave from cluster\n");
  for (int i = 0; i < 10; i++) {
    f1->Write(mystr[i], mystr[i]);
    s = f1->Read(mystr[i], &val);
    printf("status %s val %s\n", s.ToString().c_str(), val.c_str());
  }


  delete f2;
  delete f3;
  delete f1;
  return 0;
}
