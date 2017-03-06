#ifndef __FLOYD_SERVER_H__
#define __FLOYD_SERVER_H__
#include "logger.h"
#include "include/pb_conn.h"
#include "include/holy_thread.h"
#include "floyd.h"
#include "bada_sdk.pb.h"

class FloydServer {
 public:
  std::string listen_ip_;
  int listen_port_;
  floyd::Options floyd_option_;
  floyd::Floyd* floyd_;
  Mutex server_mutex;

  FloydServer(std::string listen_ip, int listen_port_,
              floyd::Options floyd_option);
  virtual ~FloydServer();
  floyd::Status Start();

 private:
  class FloydServerThread;
  class FloydServerConn : public pink::PbConn {
   public:
    FloydServerConn(int fd, const std::string& ip_port, pink::Thread* thread);
    virtual ~FloydServerConn();
    virtual pink::Status BuildObuf();
    virtual int DealMessage();

    FloydServerThread* server_thread_;
    uint32_t ret_opcode_;
  };

  class FloydServerThread : public pink::HolyThread<FloydServerConn> {
   public:
    explicit FloydServerThread(int port, FloydServer* server_);
    virtual ~FloydServerThread();
    FloydServer* server_;
  };

  FloydServerThread* server_thread_;

 public:
  friend FloydServerConn;
  friend FloydServerThread;
};
#endif
