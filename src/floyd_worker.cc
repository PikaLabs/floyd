#include "floyd_worker.h"

#include "floyd.h"
#include "floyd_mutex.h"
#include "floyd_util.h"
#include "command.pb.h"
#include "logger.h"

namespace floyd {

FloydWorkerCliConn::FloydWorkerCliConn(const std::string& ip, const int port)
    : local_ip_(ip), local_port_(port) {}

FloydWorkerCliConn::~FloydWorkerCliConn() {}

Status FloydWorkerCliConn::Connect() {
  pink::Status ret = PbCli::Connect(local_ip_, local_port_);
  return ParseStatus(ret);
}

Status FloydWorkerCliConn::GetResMessage(google::protobuf::Message* cmd_res) {
  pink::Status ret = Recv(cmd_res);
  return ParseStatus(ret);
}

Status FloydWorkerCliConn::SendMessage(google::protobuf::Message* cmd) {
  pink::Status ret = Send(cmd);
 // LOG_DEBUG("FloydWorkerCliConn Send to ip:  %s, port : %d, res: %s", local_ip_.c_str(), local_port_, ret.ToString().c_str());
  return ParseStatus(ret);
}

FloydWorkerConn::FloydWorkerConn(int fd, std::string& ip_port,
                                 pink::Thread* thread)
    : PbConn(fd, ip_port) {}

FloydWorkerConn::~FloydWorkerConn() {}

int FloydWorkerConn::DealMessage() {
  if (!command_.ParseFromArray(rbuf_ + 4, header_len_)) {
    LOG_DEBUG("WorkerConn::DealMessage ParseFromArray failed");
    return -1;
  }
  command_res_.Clear();

  switch (command_.type()) {
    case command::Command::DirtyWrite: {
      LOG_DEBUG("WorkerThread::DealMessage DirtyWrite");
      set_is_reply(false);
      command::Command_Kv kv = command_.kv();
      std::string key = kv.key();
      std::string value = kv.value();
      Status ret = Floyd::db->Set(key, value);
      if (!ret.ok()) break;
    }
    case command::Command::Write: {
      LOG_DEBUG("WorkerThread::DealMessage Write");
      set_is_reply(true);
      Status ret = Floyd::raft_->HandleWriteCommand(command_);
      command_res_.set_type(command::CommandRes::Write);
      command::CommandRes_KvRet* kvr = new command::CommandRes_KvRet();
      if (!ret.ok())
        kvr->set_status(false);
      else
        kvr->set_status(true);
      // printf ("Write end key(%s) v(%s) ret(%s)\n", kvr->key
      command_res_.set_allocated_kvr(kvr);
      break;
    }
    case command::Command::Delete: {
      LOG_DEBUG("WorkerThread::DealMessage Delete");
      set_is_reply(true);
      Status ret = Floyd::raft_->HandleDeleteCommand(command_);
      command_res_.set_type(command::CommandRes::Delete);
      command::CommandRes_KvRet* kvr = new command::CommandRes_KvRet();
      if (!ret.ok())
        kvr->set_status(false);
      else
        kvr->set_status(true);
      // printf ("Write end key(%s) v(%s) ret(%s)\n", kvr->key
      command_res_.set_allocated_kvr(kvr);
      break;
    }
    case command::Command::Read: {
      LOG_DEBUG("WorkerThread::DealMessage Read as Leader from Follower redirect: %s",
                ip_port().c_str());
      set_is_reply(true);
      std::string value;
      Status ret = Floyd::raft_->HandleReadCommand(command_, value);
      command_res_.set_type(command::CommandRes::Read);
      command::CommandRes_KvRet* kvr = new command::CommandRes_KvRet();
      if (!ret.ok()) {
        kvr->set_status(false);
      } else {
        kvr->set_status(true);
        kvr->set_value(value);
      }
      command_res_.set_allocated_kvr(kvr);
      break;
    }
    case command::Command::ReadAll: {
      LOG_DEBUG("WorkerThread::DealMessage ReadAll");
      set_is_reply(true);
      KVMap kvMap;
      Status ret = Floyd::raft_->HandleReadAllCommand(command_, kvMap);
      //@TODO construct return value,parse from map
      command_res_.set_type(command::CommandRes::ReadAll);
      command::CommandRes_KvAllRet* kvAllRet =
          new command::CommandRes_KvAllRet();
      if (!ret.ok()) {
        kvAllRet->set_status(false);
      } else {
        kvAllRet->set_status(true);
        KVMap::iterator it;
        for (it = kvMap.begin(); it != kvMap.end(); ++it) {
          command::CommandRes_Kv kv = command::CommandRes_Kv();
          kv.set_key(it->first);
          kv.set_value(it->first);
          *(kvAllRet->add_kvall()) = kv;
        }
      }
      command_res_.set_allocated_kvallr(kvAllRet);
      break;
    }

    case command::Command::TryLock: {
      LOG_DEBUG("Workerhread::DealMessage TryLock as Leader from Follower redirect: %s",
                ip_port().c_str());
      Status ret = Floyd::raft_->HandleTryLockCommand(command_);

      set_is_reply(true);
      command_res_.set_type(command::CommandRes::TryLock);

      command::CommandRes_KvRet* kvr = new command::CommandRes_KvRet();
//      if (!ret.ok() && !ret.IsTimeOut()) {
      if (!ret.ok()) {
        // printf ("TryLock error as leader:%s\n", ret.ToString().c_str());
        kvr->set_status(false);
      } else {
        kvr->set_status(true);
        kvr->set_value(ret.ToString());
        // printf ("return TryLock result to follower result:%s\n",
        // ret.ToString().c_str());
      }

      command_res_.set_allocated_kvr(kvr);
      break;
    }
    case command::Command::UnLock: {
      LOG_DEBUG("Workerhread::DealMessage UnLock as Leader from Follower redirect: %s",
                ip_port().c_str());

      Status ret = Floyd::raft_->HandleUnLockCommand(command_);

      set_is_reply(true);
      command_res_.set_type(command::CommandRes::UnLock);

      command::CommandRes_KvRet* kvr = new command::CommandRes_KvRet();
      if (!ret.ok() && !ret.IsTimeout()) {
        // printf ("UnLock error as leader:%s\n", ret.ToString().c_str());
        kvr->set_status(false);
      } else {
        kvr->set_status(true);
        kvr->set_value(ret.ToString());
        // printf ("return UnLock result to follower result:%s\n",
        // ret.ToString().c_str());
      }

      command_res_.set_allocated_kvr(kvr);
      break;
    }
    case command::Command::RaftVote: {
      LOG_DEBUG("WorkerThread::DealMessage RaftVote");
      set_is_reply(true);
      Floyd::raft_->HandleRequestVote(command_, &command_res_);
      break;
    }
    case command::Command::RaftAppendEntries: {
      LOG_DEBUG("WorkerThread::DealMessage RaftVote");
      set_is_reply(true);
      Floyd::raft_->HandleAppendEntries(command_, &command_res_);
      break;
    }
    case command::Command::SynRaftStage: {
      LOG_DEBUG("WorkerThread::DealMessage SynRaftStage");
      set_is_reply(true);
      command::CommandRes_RaftStageRes* rsr =
          new command::CommandRes_RaftStageRes();
      Floyd::raft_->HandleGetServerStatus(*rsr);
      command_res_.set_type(command::CommandRes::SynRaftStage);
      command_res_.set_allocated_raftstage(rsr);
      break;
    }

    default:
      break;
  }

  res_ = &command_res_;

  return 0;
}

FloydWorkerThread::FloydWorkerThread(int port)
    : HolyThread<FloydWorkerConn>(port) {}

FloydWorkerThread::~FloydWorkerThread() {}

// Only connection from other node should be accepted
bool FloydWorkerThread::AccessHandle(std::string& ip_port) {
  LOG_DEBUG("WorkerThread::AccessHandle start check(%s)", ip_port.c_str());
  MutexLock l(&Floyd::nodes_mutex);
  for (auto it = Floyd::nodes_info.begin(); it != Floyd::nodes_info.end(); it++) {
    if (ip_port.find((*it)->ip) != std::string::npos) {
      LOG_DEBUG("WorkerThread::AccessHandle ok, with a node(%s:%d)",
                (*it)->ip.c_str(), (*it)->port);
      return true;
    }
  }
  LOG_DEBUG("WorkerThread::AccessHandle failed");
  return false;
}

} // namespace floyd
