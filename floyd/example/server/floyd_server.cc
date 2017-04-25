#include "floyd_server.h"

#include <google/protobuf/text_format.h>

#include "client.pb.h"
#include "floyd/src/logger.h"

#include "pink/include/server_thread.h"

namespace floyd {

FloydServer::FloydServer(int sdk_port, const Options& options)
  : options_(options) {
  floyd_ = new Floyd(options_);
  conn_factory_ = new FloydServerConnFactory(floyd_); 
  server_thread_ = pink::NewHolyThread(sdk_port, conn_factory_);
  LOG_INFO ("FloydServer will started on port:%d", sdk_port);
  //pthread_rwlock_init(&state_protector_, NULL);
}

FloydServer::~FloydServer() {
  delete server_thread_;
  delete conn_factory_;
  delete floyd_;
}

Status FloydServer::Start() {
  Status result = floyd_->Start();
  if (!result.ok()) {
    LOG_INFO ("Floyd started failed, %s", result.ToString().c_str());
    return result;
  }

  server_thread_->StartThread();
  LOG_INFO ("Floyd started on port:%d", options_.local_port);
  server_mutex.Lock();
  server_mutex.Lock();
  return Status::OK();
}

////// ServerConn //////
FloydServerConn::FloydServerConn(int fd, const std::string &ip_port, pink::Thread *thread,
                                 Floyd *floyd)
    : PbConn(fd, ip_port, thread),
      floyd_(floyd) {
}

int FloydServerConn::DealMessage() {
  if (!command_.ParseFromArray(rbuf_ + 4, header_len_)) {
    LOG_DEBUG("WorkerConn::DealMessage ParseFromArray failed");
    return -1;
  }
  command_res_.Clear();

  LOG_INFO ("deal message msg_code:%d\n", command_.type());

  set_is_reply(true);

  switch (command_.type()) {
    case client::Type::WRITE: {
      LOG_DEBUG("ServerConn::DealMessage Write");
      client::Request_Write request = command_.write();

      command_res_.set_type(client::Type::WRITE);
      client::Response_Write* response = command_res_.mutable_write();

      Status result = floyd_->Write(request.key(), request.value());
      if (!result.ok()) {
        response->set_status(1);
        response->set_msg(result.ToString());
        LOG_ERROR("write failed %s", result.ToString().c_str());
      } else {
        response->set_status(0);
        LOG_INFO ("write key(%s) ok!", request.key().c_str());
      }
      break;
    }

    case client::Type::READ: {
      LOG_DEBUG("ServerConn::DealMessage READ");
      client::Request_Read request = command_.read();

      command_res_.set_type(client::Type::READ);
      client::Response_Read* response = command_res_.mutable_read();

      std::string value;
      Status result = floyd_->Read(request.key(), value);
      if (!result.ok()) {
        response->set_status(1);
        LOG_ERROR("read failed %s", result.ToString().c_str());
      } else if (result.ok()) {
        response->set_status(0);
        response->set_value(value);
        LOG_INFO ("read key(%s) ok!", request.key().c_str());
      }

      std::string text_format;
      google::protobuf::TextFormat::PrintToString(command_res_, &text_format);
      LOG_DEBUG("Read res message :\n%s", text_format.c_str());
      break;
    }

    case client::Type::STATUS: {
      LOG_DEBUG("ServerConn::DealMessage ServerStaus");
      command_res_.set_type(client::Type::STATUS);
      client::Response_ServerStatus* response = command_res_.mutable_server_status();

      std::string value;
      bool ret = floyd_->GetServerStatus(value);
      if (!ret) {
        response->set_msg("failed to dump status");
        LOG_ERROR("Status failed");
      } else {
        response->set_msg(value);
        LOG_INFO ("Status ok!\n%s", value.c_str());
      }
      break;
    }
    case client::Type::DIRTYWRITE: {
      LOG_DEBUG("ServerConn::DealMessage DirtyWrite");
      client::Request_Write request = command_.write();

      command_res_.set_type(client::Type::DIRTYWRITE);
      client::Response_Write* response = command_res_.mutable_write();

      Status result = floyd_->DirtyWrite(request.key(), request.value());
      if (!result.ok()) {
        response->set_status(1);
        response->set_msg(result.ToString());
        LOG_ERROR("DirtyWrite failed %s", result.ToString().c_str());
      } else {
        response->set_status(0);
        LOG_INFO ("DirtyWrite key(%s) ok!", request.key().c_str());
      }

      std::string text_format;
      google::protobuf::TextFormat::PrintToString(command_res_, &text_format);
      LOG_DEBUG("DirtyWrite res message :\n%s", text_format.c_str());
      break;
    }
    case client::Type::DIRTYREAD: {
      LOG_DEBUG("ServerConn::DealMessage DIRTYREAD");
      client::Request_Read request = command_.read();

      command_res_.set_type(client::Type::DIRTYREAD);
      client::Response_Read* response = command_res_.mutable_read();

      std::string value;
      Status result = floyd_->DirtyRead(request.key(), value);
      if (!result.ok()) {
        response->set_status(1);
        LOG_ERROR("DirtyRead failed %s", result.ToString().c_str());
      } else if (result.ok()) {
        response->set_status(0);
        response->set_value(value);
        LOG_INFO ("DirtyRead key(%s) ok!", request.key().c_str());
      }

      std::string text_format;
      google::protobuf::TextFormat::PrintToString(command_res_, &text_format);
      LOG_DEBUG("DirtyRead res message :\n%s", text_format.c_str());
      break;
    }
    default:
      LOG_INFO ("invalid msg_code %d\n", command_.type());
      break;
  }

  res_ = &command_res_;
  return 0;
}

} // namespace floyd
