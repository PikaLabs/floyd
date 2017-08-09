// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_worker.h"

#include <google/protobuf/text_format.h>

#include "floyd/src/floyd_impl.h"
#include "floyd/src/logger.h"

namespace floyd {
using slash::Status;

FloydWorker::FloydWorker(int port, int cron_interval, FloydImpl* floyd)
  : conn_factory_(floyd),
    handle_(floyd) {
    thread_ = pink::NewHolyThread(port, &conn_factory_, cron_interval, &handle_);
}

FloydWorkerConn::FloydWorkerConn(int fd, const std::string& ip_port,
    pink::ServerThread* thread, FloydImpl* floyd)
  : PbConn(fd, ip_port, thread),
  floyd_(floyd){
  }

FloydWorkerConn::~FloydWorkerConn() {}

int FloydWorkerConn::DealMessage() {
  if (!request_.ParseFromArray(rbuf_ + 4, header_len_)) {
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(request_, &text_format);
    LOGV(WARN_LEVEL, floyd_->info_log_, "FloydWorker:DealMessage :\n%s \n", text_format.c_str());
    LOGV(WARN_LEVEL, floyd_->info_log_, "DealMessage ParseFromArray failed");
    return -1;
  }
  response_.Clear();
  set_is_reply(true);

  switch (request_.type()) {
    case Type::kWrite:
    case Type::kDelete:
    case Type::kRead: {
      LOGV(DEBUG_LEVEL, floyd_->info_log_, "WorkerConn::DealMessage Write/Delete/Read");
      floyd_->DoCommand(request_, &response_);
      break;
    }
    case Type::kDirtyWrite:
    case Type::kServerStatus: {
      LOGV(DEBUG_LEVEL, floyd_->info_log_, "WorkerConn::DealMessage DirtyWrite/ServerStatus");
      floyd_->ReplyExecuteDirtyCommand(request_, &response_);
      break;
    }
    case Type::kRequestVote: {
      LOGV(DEBUG_LEVEL, floyd_->info_log_, "WorkerConn::DealMessage RequestVote");
      floyd_->ReplyRequestVote(request_, &response_);
      break;
    }
    case Type::kAppendEntries: {
      LOGV(DEBUG_LEVEL, floyd_->info_log_, "WorkerConn::DealMessage AppendEntries");
      floyd_->ReplyAppendEntries(request_, &response_);
      break;
    }
    default:
      LOGV(WARN_LEVEL, floyd_->info_log_, "unknown cmd type");
      return -1;
  }

  res_ = &response_;
  return 0;
}

FloydWorkerHandle::FloydWorkerHandle(FloydImpl* f)
  : floyd_(f) {
  }

// Only connection from other node should be accepted
bool FloydWorkerHandle::AccessHandle(std::string& ip_port) const {
  //TODO(anan) 
  //if (floyd_->peers_.find(ip_port) == floyd_->peers_.end()) {
  //  LOGV(WARN_LEVEL, floyd_->info_log_, "WorkerThread deny access from %s", ip_port.c_str());
  //  return false;
  //}
  return true;
}

} // namespace floyd
