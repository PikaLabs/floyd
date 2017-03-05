#ifndef __FLOYD_META_H__
#define __FLOYD_META_H__

#include "holy_thread.h"
#include "pb_conn.h"
#include "pink_cli.h"
#include "meta.pb.h"
#include "floyd_define.h"
#include "floyd_util.h"
#include "floyd_worker.h"
#include "include/slash_status.h"
#include "slice.h"
namespace floyd {
using slash::Status;
class NodeInfo;
typedef std::pair<std::string, int> NodeAddr;
struct NodeInfo {
  std::string ip;
  int port;
  time_t last_ping;
  pink::PinkCli* mcc;
  pink::PinkCli* dcc;

  NodeInfo(const std::string& ip, const int port);

  Status UpHoldWorkerCliConn(bool create_new_connect = false);
  bool operator==(NodeInfo& node_y);
};

class FloydMetaConn : public pink::PbConn {
 public:
  FloydMetaConn(int fd, const std::string& ip_port);
  FloydMetaConn(int fd, const std::string& ip_port, pink::Thread* thread);
  virtual ~FloydMetaConn();

  virtual int DealMessage();

 private:
  meta::Meta meta_;
  meta::MetaRes meta_res_;
};

class FloydMetaThread : public pink::HolyThread<FloydMetaConn> {
 public:
  explicit FloydMetaThread(int port);
  virtual ~FloydMetaThread();
};
}
#endif
