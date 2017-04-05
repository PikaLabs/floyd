#ifndef FLOYD_META_H_
#define FLOYD_META_H_

#include "floyd_worker.h"
#include "floyd_define.h"
#include "floyd_mutex.h"
#include "slice.h"

#include "slash_status.h"

#include "holy_thread.h"
#include "pb_conn.h"
#include "pb_cli.h"

using slash::Status;

namespace floyd {

class NodeInfo;
//typedef std::pair<std::string, int> NodeAddr;

struct NodeInfo {
  std::string ip;
  int port;
  time_t last_ping;
  FloydWorkerCliConn* dcc;
  Mutex dcc_mutex;

  NodeInfo(const std::string& ip, const int port);

  Status UpHoldWorkerCliConn(bool create_new_connect = false);
  bool operator==(NodeInfo& node_y);
};

} // namespace floyd
#endif
