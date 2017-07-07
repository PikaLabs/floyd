#ifndef KV_KV_SERVER_H_
#define KV_KV_SERVER_H_

#include <atomic>
#include "sdk.pb.h"

#include "pink/include/pb_conn.h"
#include "pink/include/server_thread.h"

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "floyd/include/floyd.h"

namespace floyd {

class KvServer;
class KvServerConn;
class KvServerConnFactory;

class KvServerHandler : public pink::ServerHandle {
 public:
  explicit KvServerHandler(KvServer* server)
    : server_(server) { }
  virtual ~KvServerHandler() {}

  virtual void CronHandle() const;
  virtual bool AccessHandle(std::string& ip) const {
    return true;
  }

 private:
  KvServer* server_;
};

class KvServerConn : public pink::PbConn {
 public:
  KvServerConn(int fd, const std::string& ip_port, pink::ServerThread* thread,
                  Floyd* floyd, KvServer* server);
  virtual ~KvServerConn() {}

  //virtual pink::Status BuildObuf();
  virtual int DealMessage();

 private:
  Floyd* floyd_;
  KvServer* server_;
  client::Request command_;
  client::Response command_res_;
};

class KvServerConnFactory : public pink::ConnFactory {
 public:
  explicit KvServerConnFactory(Floyd* floyd, KvServer* server)
    : floyd_(floyd),
      server_(server) { }

  virtual pink::PinkConn *NewPinkConn(int connfd,
                                      const std::string &ip_port,
                                      pink::ServerThread *server_thread,
                                      void* worker_private_data) const override {
    return new KvServerConn(connfd, ip_port, server_thread, floyd_, server_);
  }

 private:
  Floyd* floyd_;
  KvServer* server_;
};

class KvServer {
 public:
  explicit KvServer(int sdk_port, const Options& option);
  virtual ~KvServer();
  slash::Status Start();

  uint64_t last_qps() {
    return last_qps_.load();
  }

  void PlusQueryNum() {
    query_num_++;
  }

  void ResetLastSecQueryNum() {
    uint64_t cur_time_us = slash::NowMicros();
    last_qps_ = (query_num_ - last_query_num_) * 1000000 / (cur_time_us - last_time_us_ + 1);
    last_query_num_ = query_num_.load();
    last_time_us_ = cur_time_us;
  }

  slash::Mutex server_mutex;

 private:
  Options options_;

  Floyd* floyd_;

  KvServerHandler* server_handler_;
  KvServerConnFactory* conn_factory_;
  pink::ServerThread* server_thread_;

  std::atomic<uint64_t> last_query_num_;
  std::atomic<uint64_t> query_num_;
  std::atomic<uint64_t> last_qps_;
  std::atomic<uint64_t> last_time_us_;
};

} // namespace floyd
#endif
