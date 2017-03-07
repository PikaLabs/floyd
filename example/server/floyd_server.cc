#include "floyd_server.h"

#include "client.pb.h"
#include "logger.h"

//#include <glog/logging.h>

namespace floyd {

FloydServer::FloydServer(int sdk_port, const Options& options)
  : options_(options) {
  server_thread_ = new FloydServerThread(sdk_port, this);
  LOG_INFO ("FloydServer will started on port:%d", sdk_port);
  //pthread_rwlock_init(&state_protector_, NULL);
}

FloydServer::~FloydServer() {
  delete server_thread_;
}

Status FloydServer::Start() {
  floyd_ = new Floyd(options_);
  Status result = floyd_->Start();

  server_thread_->StartThread();
  LOG_INFO ("Floyd started on port:%d", options_.local_port);
  server_mutex.Lock();
  server_mutex.Lock();
  return Status::OK();
}

////// ServerConn //////
FloydServerConn::FloydServerConn(int fd, std::string &ip_port, pink::Thread *thread)
    : PbConn(fd, ip_port) {
  server_thread_ = dynamic_cast<FloydServerThread *>(thread);
}

int FloydServerConn::DealMessage() {
  uint32_t buf;
  memcpy((char *)(&buf), rbuf_ + 4, sizeof(uint32_t));
  uint32_t msg_code = ntohl(buf);
  LOG_INFO ("deal message msg_code:%d\n", msg_code);

  set_is_reply(true);

  switch (msg_code) {
    case client::OPCODE::WRITE: {
      client::Write_Request request;
      client::Write_Response* response = new client::Write_Response;
      request.ParseFromArray(rbuf_ + 8, header_len_ - 4);

      Status result = server_thread_->server_->floyd_->Write(request.key(), request.value());
      if (!result.ok()) {
        response->set_status(1);
        response->set_msg(result.ToString());
        LOG_ERROR("write failed %s", result.ToString().c_str());
      } else {
        response->set_status(0);
        LOG_INFO ("write key(%s) ok!", request.key().c_str());
      }
      res_ = response;
      break;
    }

    case client::OPCODE::READ: {
      client::Read_Request request;
      client::Read_Response* response = new client::Read_Response;
      request.ParseFromArray(rbuf_ + 8, header_len_ - 4);

      std::string value;
      Status result = server_thread_->server_->floyd_->Read(request.key(), value);
      if (!result.ok()) {
        response->set_status(1);
        LOG_ERROR("read failed %s", result.ToString().c_str());
      } else if (result.ok()) {
        response->set_status(0);
        response->set_value(value);
        LOG_INFO ("read key(%s) ok!", request.key().c_str());
      }
      res_ = response;
      break;
    }

    default:
      LOG_INFO ("invalid msg_code %d\n", msg_code);
      break;
  }

  return 0;
}

////// ServerThread //////
FloydServerThread::FloydServerThread(int port, FloydServer *server)
    : HolyThread<FloydServerConn>(port) {
  server_ = server;
}

} // namespace floyd
