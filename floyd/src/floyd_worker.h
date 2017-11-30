// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef FLOYD_SRC_FLOYD_WORKER_H_
#define FLOYD_SRC_FLOYD_WORKER_H_

#include <string>

#include "floyd/src/floyd.pb.h"
#include "pink/include/server_thread.h"
#include "pink/include/pb_conn.h"

namespace floyd {

class FloydImpl;
class FloydWorkerConnFactory;
class FloydWorkerHandle;

class FloydWorkerConn : public pink::PbConn {
 public:
  FloydWorkerConn(int fd, const std::string& ip_port,
      pink::ServerThread* thread, FloydImpl* floyd);
  virtual ~FloydWorkerConn();

  virtual int DealMessage();

 private:
  FloydImpl* const floyd_;
  CmdRequest request_;
  CmdResponse response_;
};

class FloydWorkerConnFactory : public pink::ConnFactory {
 public:
  explicit FloydWorkerConnFactory(FloydImpl* floyd)
    : floyd_(floyd) {}

  pink::PinkConn *NewPinkConn(int connfd, const std::string &ip_port,
      pink::ServerThread *server_thread, void* worker_private_data) const override {
    return new FloydWorkerConn(connfd, ip_port, server_thread, floyd_);
  }

 private:
  FloydImpl* const floyd_;
};

class FloydWorkerHandle : public pink::ServerHandle {
 public:
  explicit FloydWorkerHandle(FloydImpl* f);
  using pink::ServerHandle::AccessHandle;
  bool AccessHandle(std::string& ip) const override;
 private:
  FloydImpl* const floyd_;
};

class FloydWorker {
 public:
  FloydWorker(int port, int cron_interval, FloydImpl* floyd);

  ~FloydWorker() {
    // thread_->StopThread();
    delete thread_;
  }

  int Start() {
    return thread_->StartThread();
  }

  int Stop() {
    return thread_->StopThread();
  }

 private:
  FloydWorkerConnFactory conn_factory_;
  FloydWorkerHandle handle_;
  pink::ServerThread* thread_;
};

}  // namespace floyd
#endif  // FLOYD_SRC_FLOYD_WORKER_H_
