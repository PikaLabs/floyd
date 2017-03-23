#ifndef FLOYD_SERVER_H
#define FLOYD_SERVER_H

#include "client.pb.h"

#include "floyd.h"

#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

namespace floyd {

class FloydServer;
class FloydServerConn;
class FloydServerThread;

class FloydServer {
 public:
  explicit FloydServer(int sdk_port, const Options& option);
  virtual ~FloydServer();
  Status Start();
  
  friend class FloydServerConn;
  friend class FloydServerThread;

 private:
  Options options_;

  Floyd* floyd_;
  Mutex server_mutex;

  FloydServerThread* server_thread_;

};

class FloydServerConn : public pink::PbConn {
 public:
  FloydServerConn(int fd, std::string& ip_port, pink::Thread* thread);
  virtual ~FloydServerConn() {}

  //virtual pink::Status BuildObuf();
  virtual int DealMessage();

 private:
  FloydServerThread* server_thread_;
  client::Request command_;
  client::Response command_res_;
};

class FloydServerThread : public pink::HolyThread<FloydServerConn> {
 public:
  explicit FloydServerThread(int port, FloydServer* server_);
  virtual ~FloydServerThread() {}
  FloydServer* server_;
};

} // namespace floyd
#endif
