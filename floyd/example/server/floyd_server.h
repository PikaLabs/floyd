#ifndef FLOYD_SERVER_H
#define FLOYD_SERVER_H

#include "floyd/include/floyd.h"
#include "client.pb.h"

#include "pink/include/pb_conn.h"
#include "pink/include/server_thread.h"

#include "slash/include/slash_mutex.h"

namespace floyd {

class FloydServer;
class FloydServerConn;
class FloydServerConnFactory;

class FloydServerConn : public pink::PbConn {
 public:
  FloydServerConn(int fd, const std::string& ip_port, pink::Thread* thread,
                  Floyd* floyd);
  virtual ~FloydServerConn() {}

  //virtual pink::Status BuildObuf();
  virtual int DealMessage();

 private:
  Floyd* floyd_;
  client::Request command_;
  client::Response command_res_;
};

class FloydServerConnFactory : public pink::ConnFactory {
 public:
  explicit FloydServerConnFactory(Floyd* floyd)
    : floyd_(floyd) { }

  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string &ip_port, pink::Thread *thread) const override {
    return new FloydServerConn(connfd, ip_port, thread, floyd_);
  }

 private:
  Floyd* floyd_;
};

class FloydServer {
 public:
  explicit FloydServer(int sdk_port, const Options& option);
  virtual ~FloydServer();
  Status Start();

 private:
  Options options_;

  Floyd* floyd_;
  slash::Mutex server_mutex;

  FloydServerConnFactory* conn_factory_;
  pink::ServerThread* server_thread_;
};

} // namespace floyd
#endif
