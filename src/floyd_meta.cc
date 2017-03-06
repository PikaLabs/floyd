#include "include/floyd_mutex.h"
#include "include/floyd_meta.h"
#include "include/floyd.h"
#include "meta.pb.h"
#include "include/logger.h"
namespace floyd {

NodeInfo::NodeInfo(const std::string& ip, const int port) {
  this->ip = ip;
  this->port = port;
  this->last_ping = time(NULL);
  this->mcc = NULL;
  this->dcc = NULL;
}

bool NodeInfo::operator==(NodeInfo& node) {
  if ((this->ip == node.ip) && (this->port == node.port)) return true;
  return false;
}

Status NodeInfo::UpHoldWorkerCliConn(bool create_new_connect) {
  Status ret = Status::OK();
  if (create_new_connect || dcc == NULL) {
    if (dcc != NULL) {
      dcc->Close();
      delete dcc;
    }
    dcc = pink::NewPbCli();
    ret = dcc->Connect(ip, port + 100);
  }
  return ret;
}

FloydMetaConn::FloydMetaConn(int fd, const std::string& ip_port)
    : PbConn(fd, ip_port) {}

FloydMetaConn::FloydMetaConn(int fd, const std::string& ip_port, pink::Thread* thread)
    : PbConn(fd, ip_port) {}
FloydMetaConn::~FloydMetaConn() {}

int FloydMetaConn::DealMessage() {
  meta_.ParseFromArray(rbuf_ + 4, header_len_);
  MutexLock l(&Floyd::nodes_mutex);
  if (meta_.t() == meta::Meta::NODE) {
    std::vector<NodeInfo*>::iterator iter;
    set_is_reply(true);
    for (int i = 0; i < meta_.nodes_size(); i++) {
      meta::Meta_Node node = meta_.nodes(i);
      for (iter = Floyd::nodes_info.begin(); iter != Floyd::nodes_info.end();
           ++iter) {
        if ((*iter)->ip == node.ip() && (*iter)->port == node.port()) {
          break;
        }
      }
      if (iter == Floyd::nodes_info.end()) {
        NodeInfo* ni = new NodeInfo(node.ip(), node.port());
        ni->dcc = pink::NewPbCli();
        ni->dcc->Connect(ni->ip, ni->port + 100);
        Floyd::nodes_info.push_back(ni);
        Floyd::raft_con->AddNewPeer(ni);
        LOG_DEBUG("MetaThread::DealMessage: find a new node: %s:%d",
                  ni->ip.c_str(), ni->port);
      }
    }
    meta_res_.clear_nodes();
    for (iter = Floyd::nodes_info.begin(); iter != Floyd::nodes_info.end();
         ++iter) {
      meta::MetaRes_Node* node = meta_res_.add_nodes();
      node->set_ip((*iter)->ip);
      node->set_port((*iter)->port);
    }

    res_ = &meta_res_;
  }

  if (meta_.t() == meta::Meta::PING) {
    set_is_reply(false);
    assert(meta_.nodes_size() == 1);
    meta::Meta_Node node = meta_.nodes(0);
    std::vector<NodeInfo*>::iterator iter = Floyd::nodes_info.begin();
    for (; iter != Floyd::nodes_info.end(); ++iter) {
      if ((*iter)->ip == node.ip() && (*iter)->port == node.port()) {
        break;
      }
    }
    if (iter != Floyd::nodes_info.end()) {
      (*iter)->last_ping = time(NULL);
    }
  }

  return 0;
}

FloydMetaThread::FloydMetaThread(int port) : HolyThread<FloydMetaConn>(port) {}

FloydMetaThread::~FloydMetaThread() {}
}
