#ifndef FLOYD_SERVER_H
#define FLOYD_SERVER_H

#include <atomic>
#include "floyd/include/floyd.h"
#include "client.pb.h"

#include "pink/include/pb_conn.h"
#include "pink/include/server_thread.h"

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

namespace floyd {

class FloydServer;
class FloydServerConn;
class FloydServerConnFactory;

class FloydServerHandler : public pink::ServerHandle {
 public:
  explicit FloydServerHandler(FloydServer* server)
    : server_(server) { }
  virtual ~FloydServerHandler() {}

  virtual void CronHandle() const;
  virtual bool AccessHandle(std::string& ip) const {
    return true;
  }

 private:
  FloydServer* server_;
};

class FloydServerConn : public pink::PbConn {
 public:
  FloydServerConn(int fd, const std::string& ip_port, pink::Thread* thread,
                  Floyd* floyd, FloydServer* server);
  virtual ~FloydServerConn() {}

  //virtual pink::Status BuildObuf();
  virtual int DealMessage();

 private:
  FloydServer* server_;
  Floyd* floyd_;
  client::Request command_;
  client::Response command_res_;
};

class FloydServerConnFactory : public pink::ConnFactory {
 public:
  explicit FloydServerConnFactory(Floyd* floyd, FloydServer* server)
    : floyd_(floyd),
      server_(server) { }

  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string &ip_port, pink::Thread *thread) const override {
    return new FloydServerConn(connfd, ip_port, thread, floyd_, server_);
  }

 private:
  Floyd* floyd_;
  FloydServer* server_;
};

class FloydServer {
 public:
  explicit FloydServer(int sdk_port, const Options& option);
  virtual ~FloydServer();
  slash::Status Start();

  uint64_t last_qps() {
    return last_qps_.load();
  }

  void PlusQueryNum() {
    query_num_++;
  }

  void ResetLastSecQueryNum() {
    uint64_t cur_time_us = slash::NowMicros();
    last_qps_ = (query_num_ - last_query_num_) * 1000000 / (cur_time_us - last_time_us_ + 1);
    last_query_num_ = query_num_.load();
    last_time_us_ = cur_time_us;
  }

 private:
  Options options_;

  Floyd* floyd_;
  slash::Mutex server_mutex;

  FloydServerHandler* server_handler_;
  FloydServerConnFactory* conn_factory_;
  pink::ServerThread* server_thread_;

  std::atomic<uint64_t> query_num_;
  std::atomic<uint64_t> last_query_num_;
  std::atomic<uint64_t> last_qps_;
  std::atomic<uint64_t> last_time_us_;
};

} // namespace floyd
#endif
