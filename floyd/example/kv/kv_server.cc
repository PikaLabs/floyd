#include "./kv_server.h"

#include <google/protobuf/text_format.h>

#include "pink/include/server_thread.h"

#include "./sdk.pb.h"
#include "./logger.h"


namespace floyd {


// FloydServerHandler
void KvServerHandler::CronHandle() const {
  server_->ResetLastSecQueryNum();
  LOG_INFO ("FloydServer QPS:%llu", server_->last_qps());
}

KvServer::KvServer(int sdk_port, const Options& options)
  : options_(options),
    last_query_num_(0),
    query_num_(0),
    last_time_us_(slash::NowMicros()) {
  Floyd::Open(options_, &floyd_);
  conn_factory_ = new KvServerConnFactory(floyd_, this); 
  server_handler_ = new KvServerHandler(this);
  server_thread_ = pink::NewHolyThread(sdk_port, conn_factory_, 3000, server_handler_);
  LOG_DEBUG ("KvServer will started on port:%d", sdk_port);
  //pthread_rwlock_init(&state_protector_, NULL);
}

KvServer::~KvServer() {
  server_thread_->StopThread();
  delete server_thread_;
  delete conn_factory_;
  delete server_handler_;
  delete floyd_;
}

slash::Status KvServer::Start() {
  // TEST
  std::set<std::string> nodes;
  floyd_->GetAllNodes(nodes);
  for (auto& it : nodes) {
    LOG_DEBUG ("nodes: %s", it.c_str());
  }

  std::string ip;
  int port;
  floyd_->GetLeader(&ip, &port);
  LOG_DEBUG ("Leader: %s %d", ip.c_str(), port);

  int ret = server_thread_->StartThread();
  if (ret != 0) {
    return Status::Corruption("Start server server error");
  }
  LOG_DEBUG ("Kv started on port:%d", options_.local_port);
  server_mutex.Lock();
  server_mutex.Lock();
  server_mutex.Unlock();
  return Status::OK();
}

////// ServerConn //////
KvServerConn::KvServerConn(int fd, const std::string &ip_port,
                                 pink::ServerThread *thread,
                                 Floyd *floyd, KvServer* server)
    : PbConn(fd, ip_port, thread),
      floyd_(floyd),
      server_(server) {
}

int KvServerConn::DealMessage() {
  if (!command_.ParseFromArray(rbuf_ + 4, header_len_)) {
    LOG_DEBUG("WorkerConn::DealMessage ParseFromArray failed");
    return -1;
  }
  command_res_.Clear();

  LOG_DEBUG ("deal message msg_code:%d\n", command_.type());
  server_->PlusQueryNum();

  set_is_reply(true);

  switch (command_.type()) {
    case client::Type::WRITE: {
      LOG_DEBUG("ServerConn::DealMessage Write");
      client::Request_Write request = command_.write();

      command_res_.set_type(client::Type::WRITE);

      Status result = floyd_->Write(request.key(), request.value());
      if (!result.ok()) {
        command_res_.set_code(client::StatusCode::kError);
        command_res_.set_msg(result.ToString());
        LOG_ERROR("write failed %s", result.ToString().c_str());
      } else {
        command_res_.set_code(client::StatusCode::kOk);
        LOG_DEBUG ("write key(%s) ok!", request.key().c_str());
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
      if (result.ok()) {
        command_res_.set_code(client::StatusCode::kOk);
        response->set_value(value);
        LOG_DEBUG ("read key(%s) ok!", request.key().c_str());
      } else if (result.IsNotFound()) {
        command_res_.set_code(client::StatusCode::kNotFound);
        LOG_ERROR("read key(%s) not found %s", request.key().c_str(), result.ToString().c_str());
      } else {
        command_res_.set_code(client::StatusCode::kError);
        LOG_ERROR("read key(%s) failed %s", request.key().c_str(), result.ToString().c_str());
      }
#ifndef NDEBUG
      std::string text_format;
      google::protobuf::TextFormat::PrintToString(command_res_, &text_format);
      LOG_DEBUG("Read res message :\n%s", text_format.c_str());
#endif
      break;
    }

    case client::Type::STATUS: {
      LOG_DEBUG("ServerConn::DealMessage ServerStaus");
      command_res_.set_type(client::Type::STATUS);
      client::Response_ServerStatus* response = command_res_.mutable_server_status();

      std::string value;
      bool ret = floyd_->GetServerStatus(value);
      if (!ret) {
        command_res_.set_code(client::StatusCode::kError);
        response->set_msg("failed to dump status");
        LOG_ERROR("Status failed");
      } else {
        command_res_.set_code(client::StatusCode::kOk);
        response->set_msg(value);
        LOG_DEBUG ("Status ok!\n%s", value.c_str());
      }
      break;
    }
    case client::Type::DIRTYWRITE: {
      LOG_DEBUG("ServerConn::DealMessage DirtyWrite");
      client::Request_Write request = command_.write();

      command_res_.set_type(client::Type::DIRTYWRITE);
      Status result = floyd_->DirtyWrite(request.key(), request.value());
      if (!result.ok()) {
        command_res_.set_code(client::StatusCode::kError);
        command_res_.set_msg(result.ToString());
        LOG_ERROR("DirtyWrite failed %s", result.ToString().c_str());
      } else {
        command_res_.set_code(client::StatusCode::kOk);
        LOG_DEBUG ("DirtyWrite key(%s) ok!", request.key().c_str());
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
      if (result.ok()) {
        command_res_.set_code(client::StatusCode::kOk);
        response->set_value(value);
        LOG_DEBUG ("DirtyRead key(%s) ok!", request.key().c_str());
      } else if (result.IsNotFound()) {
        command_res_.set_code(client::StatusCode::kNotFound);
        LOG_ERROR("DirtyRead key(%s) not found %s", request.key().c_str(), result.ToString().c_str());
      } else {
        command_res_.set_code(client::StatusCode::kError);
        LOG_ERROR("DirtyRead key(%s) failed %s", request.key().c_str(), result.ToString().c_str());
      }

      std::string text_format;
      google::protobuf::TextFormat::PrintToString(command_res_, &text_format);
      LOG_DEBUG("DirtyRead res message :\n%s", text_format.c_str());
      break;
    }
    case client::Type::DELETE: {
      LOG_DEBUG("ServerConn::DealMessage Delete");
      client::Request_Delete request = command_.del();

      command_res_.set_type(client::Type::DELETE);
      Status result = floyd_->Delete(request.key());
      if (!result.ok()) {
        command_res_.set_code(client::StatusCode::kError);
        command_res_.set_msg(result.ToString());
        LOG_ERROR("Delete failed %s", result.ToString().c_str());
      } else {
        command_res_.set_code(client::StatusCode::kOk);
        LOG_DEBUG ("Delete key(%s) ok!", request.key().c_str());
      }

      std::string text_format;
      google::protobuf::TextFormat::PrintToString(command_res_, &text_format);
      LOG_DEBUG("Delete res message :\n%s", text_format.c_str());
      break;
    }
    case client::Type::LOGLEVEL: {
      int log_level = command_.log_level();
      LOG_DEBUG("ServerConn::DealMessage LogLevel %d", log_level);
      command_res_.set_type(client::Type::LOGLEVEL);
      floyd_->set_log_level(log_level);
      command_res_.set_code(client::StatusCode::kOk);
      break;
    }
    default:
      LOG_DEBUG ("invalid msg_code %d\n", command_.type());
      break;
  }

  res_ = &command_res_;
  return 0;
}

} // namespace floyd
