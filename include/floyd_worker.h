#ifndef __FLOYD_WORKER_H__
#define __FLOYD_WORKER_H__

#include "include/pb_conn.h"
#include "include/pink_cli.h"
#include "include/holy_thread.h"
#include "src/command.pb.h"
#include "floyd_define.h"
#include "floyd_util.h"
#include "include/slash_status.h"
#include "slice.h"

namespace floyd {
using slash::Status;

class NodeInfo;

class FloydWorkerConn : public pink::PbConn {
 public:
  FloydWorkerConn(int fd, const std::string& ip_port, pink::Thread* thread);
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
};
}
#endif
