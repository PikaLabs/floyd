#ifndef FLOYD_WORKER_H_
#define FLOYD_WORKER_H_

#include "command.pb.h"
#include "floyd_define.h"
#include "floyd_db.h"
#include "status.h"
#include "slice.h"

#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

namespace floyd {

class NodeInfo;
class FloydWorkerCliConn;

class FloydWorkerCliConn : public pink::PbCli {
 public:
  FloydWorkerCliConn(const std::string& ip, const int port);
  virtual ~FloydWorkerCliConn();

  virtual Status Connect();
  virtual Status GetResMessage(google::protobuf::Message* cmd_res);
  virtual Status SendMessage(google::protobuf::Message* cmd);

  std::string local_ip_;
  int local_port_;
  command::CommandRes command_res_;
};

class FloydWorkerConn : public pink::PbConn {
 public:
  FloydWorkerConn(int fd, std::string& ip_port, pink::Thread* thread);
  virtual ~FloydWorkerConn();

  virtual int DealMessage();

 private:
  command::Command command_;
  command::CommandRes command_res_;
};

class FloydWorkerThread : public pink::HolyThread<FloydWorkerConn> {
 public:
  explicit FloydWorkerThread(int port);
  virtual ~FloydWorkerThread();

  // Only connection from other node should be accepted
  virtual bool AccessHandle(std::string& ip_port);

};
}
#endif
