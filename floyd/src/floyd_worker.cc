#include "floyd/src/floyd_worker.h"

#include "floyd/include/floyd.h"
#include "floyd/src/logger.h"

namespace floyd {
using slash::Status;

FloydWorker::FloydWorker(const FloydWorkerEnv& env)
  : conn_factory_(env.floyd),
  handle_(env.floyd) {
    thread_ = pink::NewHolyThread(env.port,
        &conn_factory_, env.cron_interval, &handle_);
  }

FloydWorkerConn::FloydWorkerConn(int fd, const std::string& ip_port,
    pink::Thread* thread, Floyd* floyd)
  : PbConn(fd, ip_port, thread),
  floyd_(floyd){
  }

FloydWorkerConn::~FloydWorkerConn() {}

int FloydWorkerConn::DealMessage() {
  if (!command_.ParseFromArray(rbuf_ + 4, header_len_)) {
    LOG_DEBUG("DealMessage ParseFromArray failed");
    return -1;
  }
  command_res_.Clear();
  set_is_reply(true);

  switch (command_.type()) {
    case command::Command::Write:
    case command::Command::Delete:
    case command::Command::Read:
      floyd_->DoCommand(command_, &command_res_);
      break;
    case command::Command::RaftVote: 
      floyd_->DoRequestVote(command_, &command_res_);
      break;
    case command::Command::RaftAppendEntries:
      floyd_->DoAppendEntries(command_, &command_res_);
      break;
    default:
      LOG_WARN("unknown cmd type");
      return -1;
  }

  res_ = &command_res_;
  return 0;
}

FloydWorkerHandle::FloydWorkerHandle(Floyd* f)
  : floyd_(f) {
  }

// Only connection from other node should be accepted
bool FloydWorkerHandle::AccessHandle(std::string& ip_port) const {
  if (floyd_->peers_.find(ip_port) == floyd_->peers_.end()) {
    LOG_WARN("WorkerThread deny access from %s", ip_port.c_str());
    return false;
  }
  return true;
}

} // namespace floyd
