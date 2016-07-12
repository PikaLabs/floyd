#include "floyd_server.h"

FloydServer::FloydServerConn::FloydServerConn(int fd, std::string &ip_port,
                                              pink::Thread *thread)
    : PbConn(fd, ip_port) {
  server_thread_ = reinterpret_cast<FloydServerThread *>(thread);
}

FloydServer::FloydServerConn::~FloydServerConn() {}
pink::Status FloydServer::FloydServerConn::BuildObuf() {
  LOG_INFO("begin buildobuf");
  uint32_t u;
  wbuf_len_ = res_->ByteSize();
  u = htonl(wbuf_len_ + 4);
  memcpy(wbuf_, &u, sizeof(uint32_t));
  u = htonl(ret_opcode_);
  memcpy(wbuf_ + 4, &u, sizeof(uint32_t));
  res_->SerializeToArray(wbuf_ + 8, wbuf_len_);
  if (res_) delete res_;
  wbuf_len_ = wbuf_len_ + 8;

  LOG_INFO("Response, opcode: %d, wbuf_pos_: %d, wbuf_len_: %d", ret_opcode_, wbuf_pos_, wbuf_len_); 

  // printf ("buildobuf ret_opcode_(%d)\n", ret_opcode_);
  return pink::Status::OK();
}
int FloydServer::FloydServerConn::DealMessage() {
  LOG_INFO("deal message now");
  set_is_reply(true);
  uint32_t buf;
  memcpy((char *)(&buf), rbuf_ + 4, sizeof(uint32_t));
  uint32_t msg_code = ntohl(buf);
  LOG_INFO("get msg code:%d", msg_code);
  // printf ("get msg code:%d\n", msg_code);

  switch (msg_code) {
    //@TODO decode by config file
    case 513: {  // sdk_set
      LOG_INFO("receive set request:%d", msg_code);
      // printf ("receive set request:%d\n", msg_code);
      SdkSet command;
      command.ParseFromArray(rbuf_ + 8, header_len_ - 4);
      std::string key = command.key();
      std::string value = command.value();
      floyd::Status ret = server_thread_->server_->floyd_->Write(key, value);
      if (ret.ok()) {
        LOG_INFO("key:%s value:%s set!", key.c_str(), value.c_str());
        // printf ("key:%s value:%s set!\n",key.c_str(),value.c_str());
      } else {
        LOG_INFO("write error: %s", ret.ToString().c_str());
        // printf ("write error\n");
      }
      //@TODO encode by config file
      ret_opcode_ = 514;
      SdkSetRet *ret_msg = new SdkSetRet();
      ret_msg->set_opcode(514);
      ret_msg->set_status(true);
      ret_msg->set_master("1");
      res_ = ret_msg;
      break;
    }
    case 518: {  // sdk_get
      LOG_INFO("receive get request:%d", msg_code);
      SdkGet command;
      command.ParseFromArray(rbuf_ + 8, header_len_ - 4);
      std::string key = command.key();
      LOG_INFO("try to get key:%s ", key.c_str());
      std::string value;
      floyd::Status ret = server_thread_->server_->floyd_->Read(key, value);
      if (ret.ok()) {
        LOG_INFO("key:%s value:%s get!", key.c_str(), value.c_str());
      } else {
        LOG_INFO("read error %s", ret.ToString().c_str());
      }
      //@TODO encode by config file
      ret_opcode_ = 519;
      SdkGetRet *ret_msg = new SdkGetRet();
      ret_msg->set_opcode(519);
      ret_msg->set_value(value);
      res_ = ret_msg;
      break;
    }
    case 526: {  // sdk_getifall
      LOG_INFO("receive get all request:%d", msg_code);
      SdkMGet command;
      command.ParseFromArray(rbuf_ + 8, header_len_ - 4);
      LOG_INFO("try to get all keys ");
      KVMap kvMap;
      floyd::Status ret = server_thread_->server_->floyd_->ReadAll(kvMap);
      ret_opcode_ = 527;
      SdkMGetRet *ret_msg = new SdkMGetRet();
      ret_msg->set_opcode(527);
      if (ret.ok()) {
        KVMap::iterator it;
        for (it = kvMap.begin(); it != kvMap.end(); ++it) {
          SdkMGetRet_KeyValue kv = SdkMGetRet_KeyValue();
          kv.set_key(it->first);
          kv.set_value(it->second);
          kv.set_status(0);
          *(ret_msg->add_rets()) = kv;
        }
      } else {
        LOG_INFO("get all keys error");
      }
      res_ = ret_msg;
      break;
    }

    case 522: {  // Cas used for TryLock
      LOG_INFO("receive TryLock request:%d", msg_code);
      SdkCas command;
      command.ParseFromArray(rbuf_ + 8, header_len_ - 4);
      std::string key = command.key();
      LOG_INFO("try to TryLock key:%s ", key.c_str());

      //@TODO encode by config file
      ret_opcode_ = 523;
      SdkCasRet *ret_msg = new SdkCasRet();
      ret_msg->set_opcode(523);

      floyd::Status ret = server_thread_->server_->floyd_->TryLock(key);
      if (ret.ok()) {
        LOG_INFO("TryLock(%s) ok", key.c_str());
        ret_msg->set_status(true);
      } else {
        LOG_INFO("TryLock(%s) failed, %s ", key.c_str(),
                 ret.ToString().c_str());
        // printf ("FloydServer::TryLock(%s) failed, %s ", key.c_str(),
        // ret.ToString().c_str());
        ret_msg->set_status(false);
        ret_msg->set_master(ret.ToString());
      }

      res_ = ret_msg;
      break;
    }

    case 520: {  // GetV used for UnLock
      LOG_INFO("receive UnLock request:%d", msg_code);
      SdkGetV command;
      command.ParseFromArray(rbuf_ + 8, header_len_ - 4);
      std::string key = command.key();
      LOG_INFO("try to UnLock key:%s ", key.c_str());

      //@TODO encode by config file
      ret_opcode_ = 521;
      SdkGetVRet *ret_msg = new SdkGetVRet();
      ret_msg->set_opcode(521);
      ret_msg->set_version(1);

      floyd::Status ret = server_thread_->server_->floyd_->UnLock(key);
      if (ret.ok()) {
        LOG_INFO("UnLock(%s) ok", key.c_str());
        // printf ("UnLock(%s) ok\n\n", key.c_str());
        ret_msg->set_value("Unlock ok");
      } else {
        LOG_INFO("UnLock(%s) failed, %s ", key.c_str(), ret.ToString().c_str());
        // printf ("FloydServer::UnLock(%s) failed, %s\n\n", key.c_str(),
        // ret.ToString().c_str());
        ret_msg->set_value("Unlock failed");
        ret_msg->set_master(ret.ToString());
      }

      res_ = ret_msg;
      break;
    }

    default:
      break;
  }

  /*
  LOG_INFO("parse data message");
        command_.ParseFromArray(rbuf_ + 4, header_len_);
  set_is_reply(true);
  command_res_.set_type(command::CommandRes::Write);
        command::CommandRes_KvRet* kvr = new command::CommandRes_KvRet();
        command_res_.set_allocated_kvr(kvr);
        res_ = &command_res_;
*/
  return 0;
}

FloydServer::FloydServerThread::FloydServerThread(int port, FloydServer *server)
    : HolyThread<FloydServerConn>(port) {
  server_ = server;
  LOG_INFO("floyd node listen on port:%d", port);
}

FloydServer::FloydServerThread::~FloydServerThread() {}

FloydServer::FloydServer(std::string listen_ip, int listen_port,
                         floyd::Options floyd_option) {
  listen_ip_ = listen_ip;
  listen_port_ = listen_port;
  floyd_option_ = floyd_option;
  server_thread_ = new FloydServerThread(listen_port, this);
}

FloydServer::~FloydServer() {}

floyd::Status FloydServer::Start() {

  LOG_INFO("try to start floyd");
  floyd_ = new floyd::Floyd(floyd_option_);
  floyd::Status ret = floyd_->Start();
  server_thread_->StartThread();
  server_mutex.Lock();
  server_mutex.Lock();
  return floyd::Status::OK();
}
