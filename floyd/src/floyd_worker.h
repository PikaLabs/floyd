#ifndef FLOYD_WORKER_H_
#define FLOYD_WORKER_H_

#include "pink/include/server_thread.h"
#include "pink/include/pb_conn.h"
#include "command.pb.h"

namespace floyd {

struct FloydWorkerEnv() {
  int port;
  int cron_interval;
  Floyd floyd;
  FloydWorkerEnv(int p, int c, Floyd* f)
    : port(p),
    cron_interval(c),
    floyd(f) {}
};

class FloydWorker {
 public:
  explicit FloydWorker(const FloydWorkerEnv& env):
    conn_factory_(env.floyd){
      thread_ = pink::NewHolyThread(env.port,
          &conn_factory_, env.cron_interval, &handle_);
    }

  ~FloydWorker() {
    thread_->JoinThread();
    delete thread_;
  }

  int Start() {
    return thread_->StartThread();
  }

 private:
  pink::FloydWorkerConnFactory conn_factory_;
  pink::FloydWorkerHandle handle_;
  pink::ServerThread* thread_;
};

class FloydWorkerConn : public pink::PbConn {
 public:
  FloydWorkerConn(int fd, const std::string& ip_port,
      pink::Thread* thread, Floyd* floyd);
  virtual ~FloydWorkerConn();

  virtual int DealMessage();

 private:
  Floyd* floyd_;
  command::Command command_;
  command::CommandRes command_res_;
};

class FloydWorkerConnFactory : public pink::ConnFactory {
 public:
  explicit FloydWorkerConnFactory(Floyd* floyd)
    : floyd_(floyd) {}

  virtual PinkConn *NewPinkConn(int connfd,
      const std::string &ip_port, Thread *thread) const override {
    return new FloydWorkerConn(connfd, ip_port, thread, floyd_);
  }
 private:
  Floyd* floyd_;
};

class FloydWorkerHandle : public pink::ServerHandle {
public:
  virtual bool AccessHandle(std::string& ip) const override;
};

}
#endif
