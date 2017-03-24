#include "floyd_meta.h"

#include "floyd.h"
#include "floyd_util.h"
#include "logger.h"

namespace floyd {

NodeInfo::NodeInfo(const std::string& ip, const int port) {
  this->ip = ip;
  this->port = port;
  this->last_ping = time(NULL);
  this->dcc = NULL;
}

bool NodeInfo::operator==(NodeInfo& node) {
  return (ip == node.ip && port == node.port);
}

Status NodeInfo::UpHoldWorkerCliConn(bool create_new_connect) {
  Status ret;
  if (create_new_connect || dcc == NULL || !dcc->Available()) {
    if (dcc != NULL) {
      dcc->Close();
      delete dcc;
      dcc = NULL;
    }
    dcc = new FloydWorkerCliConn(ip, port);
    ret = dcc->Connect();
    if (ret.ok()) {
      dcc->set_send_timeout(1000);
      dcc->set_recv_timeout(1000);
    }
  }
  return ret;
}

} // namespace floyd
