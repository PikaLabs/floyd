#include "floyd_hb.h"
#include "floyd.h"
namespace floyd {
FloydHeartBeatThread::FloydHeartBeatThread(const std::string& ip,
                                           const int port,
                                           const struct timespec period)
    : ip_(ip),
      port_(port),
      period_(period),
      heartbeat_cond_(&heartbeat_mutex_) {}

FloydHeartBeatThread::~FloydHeartBeatThread() {}

void* FloydHeartBeatThread::ThreadMain() {
  struct timespec when;
  struct timeval now;
  MutexLock l(&heartbeat_mutex_);
  for (;;) {
    HeartBeat();
    gettimeofday(&now, NULL);
    when.tv_sec = now.tv_sec + period_.tv_sec;
    when.tv_nsec = now.tv_usec * 1000 + period_.tv_nsec;
    heartbeat_cond_.WaitUntil(when);
  }
  // just prevent warning since holy_thread's thread main requires return value
  int* a;
  return static_cast<void*>(a);
}

void FloydHeartBeatThread::HeartBeat() {
  Status ret = Status::OK();
  meta::Meta meta;
  MutexLock l(&Floyd::nodes_mutex);
  std::vector<NodeInfo*>::iterator iter = Floyd::nodes_info.begin();
  for (; iter != Floyd::nodes_info.end(); ++iter) {
    if ((*iter)->ip != ip_ || (*iter)->port != port_) {
      if (time(NULL) - (*iter)->last_ping > 30) {
        if ((*iter)->mcc) {
          (*iter)->mcc->Close();
          delete (*iter)->mcc;
          (*iter)->mcc = NULL;
        }
        continue;
      }

      if (!((*iter)->mcc)) {
        (*iter)->mcc = pink::NewPbCli();
        ret = (*iter)->mcc->Connect(ip_, port_);
        if (!ret.ok()) {
          delete (*iter)->mcc;
          (*iter)->mcc = NULL;
          continue;
        }
      }
      meta.set_t(meta::Meta::PING);
      meta::Meta_Node* node = meta.add_nodes();
      node->set_ip(ip_);
      node->set_port(port_);
      ret = (*iter)->mcc->Send(&meta);
      if (!ret.ok()) {
        (*iter)->mcc->Close();
        delete (*iter)->mcc;
        (*iter)->mcc = NULL;
      }
    }
  }
}
}
