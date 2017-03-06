#ifndef __FLOYD_HEARTBEAT_H__
#define __FLOYD_HEARTBEAT_H__

#include "include/pink_thread.h"
#include "include/pb_conn.h"
#include "floyd_define.h"
#include "floyd_util.h"
#include "floyd_mutex.h"
#include "floyd_meta.h"
namespace floyd {
class FloydHeartBeatThread : public pink::Thread {
 public:
  FloydHeartBeatThread(const std::string& ip, const int port,
                       const struct timespec period = (struct timespec) {1, 0});
  virtual ~FloydHeartBeatThread();
  virtual void* ThreadMain();
  void HeartBeat();

 private:
  std::string ip_;
  int port_;
  struct timespec period_;

  Mutex heartbeat_mutex_;
  CondVar heartbeat_cond_;
};
}
#endif
