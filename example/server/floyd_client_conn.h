#ifndef FLOYD_CLIENT_CONN_H
#define FLOYD_CLIENT_CONN_H

//#include <glog/logging.h>
#include "pink_thread.h"


class FloydWorkerThread;

class FloydClientConn: public pink::PbConn {
public:
  FloydClientConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~FloydClientConn();

  virtual int DealMessage();
  floydWorkerThread* self_thread() {
    return self_thread_;
  }

private:
  floydWorkerThread* self_thread_;
  //std::string DoCmd(const std::string& opt);
  //std::string RestoreArgs();

};

#endif
