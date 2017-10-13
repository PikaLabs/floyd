#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>

#include "floyd/include/floyd.h"

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"

using namespace pink;
using namespace floyd;

Floyd *f;

class MyConn: public RedisConn {
 public:
  MyConn(int fd, const std::string& ip_port, ServerThread *thread,
         void* worker_specific_data);
  virtual ~MyConn();

 protected:
  virtual int DealMessage();

 private:
};

MyConn::MyConn(int fd, const std::string& ip_port,
               ServerThread *thread, void* worker_specific_data)
    : RedisConn(fd, ip_port, thread) {
  // Handle worker_specific_data ...
}

MyConn::~MyConn() {
}

int MyConn::DealMessage() {
  slash::Status s;
  std::string val;
  // set command
  std::string res;

  if (argv_.size() == 3 && (argv_[0] == "set" || argv_[0] == "SET")) {
    int retries = 5;
    while (true) {
      retries--;
      s = f->Write(argv_[1], argv_[2]);
      if (s.ok()) {
        res = "+OK\r\n";
        break;
      } else if (retries > 0 &&
                 (s.ToString().find("no leader") != std::string::npos ||
                 s.ToString().find("Client sent error") != std::string::npos)) {
        sleep(1);
      } else {
        res = "-ERR write " + s.ToString() + " \r\n";
        break;
      }
    }
    memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
    wbuf_len_ += res.size();
  } else if (argv_.size() == 2 && (argv_[0] == "get" || argv_[0] == "GET")) {
    int retries = 5;
    while (true) {
      retries--;
      s = f->Read(argv_[1], &val);
      if (s.ok() || s.IsNotFound()) {
        if (s.IsNotFound()) {
          val = "0";  // default value for jepsen
        }
        memcpy(wbuf_ + wbuf_len_, "$", 1);
        wbuf_len_ += 1;
        std::string len = std::to_string(val.length());
        memcpy(wbuf_ + wbuf_len_, len.data(), len.size());
        wbuf_len_ += len.size();
        memcpy(wbuf_ + wbuf_len_, "\r\n", 2);
        wbuf_len_ += 2;
        memcpy(wbuf_ + wbuf_len_, val.data(), val.size());
        wbuf_len_ += val.size();
        memcpy(wbuf_ + wbuf_len_, "\r\n", 2);
        wbuf_len_ += 2;
        break;
      } else if (retries > 0 &&
                 (s.ToString().find("Client sent error") != std::string::npos ||
                 s.ToString().find("no leader") != std::string::npos)) {
        sleep(1);
      } else {
        res = "-ERR read " + s.ToString() + " \r\n";
        memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
        wbuf_len_ += res.size();
        break;
      }
    }
  } else {
    res = "+OK\r\n";
    memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
    wbuf_len_ += res.size();
  }
  set_is_reply(true);
  return 0;
}

class MyConnFactory : public ConnFactory {
 public:
  virtual PinkConn *NewPinkConn(int connfd, const std::string &ip_port,
                                ServerThread *thread,
                                void* worker_specific_data) const {
    return new MyConn(connfd, ip_port, thread, worker_specific_data);
  }
};

static std::atomic<bool> running(false);

static void IntSigHandle(const int sig) {
  running.store(false);
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char* argv[]) {
  Options op(argv[1], argv[2], atoi(argv[3]), argv[4]);
  op.Dump();

  slash::Status s;
  s = Floyd::Open(op, &f);
  printf("%s\n", s.ToString().c_str());

  SignalSetup();

  ConnFactory *conn_factory = new MyConnFactory();

  ServerThread* my_thread = NewHolyThread(atoi(argv[5]), conn_factory);
  if (my_thread->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  my_thread->StopThread();
  delete my_thread;
  delete conn_factory;

  return 0;
}
