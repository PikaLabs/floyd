#include "floyd.h"

#include "floyd_meta.h"
#include "floyd_db.h"
#include "floyd_rpc.h"
#include "status.h"
#include "env.h"
#include "logger.h"

#include "meta.pb.h"
#include "command.pb.h"

#include "slash_string.h"

namespace floyd {

Mutex Floyd::nodes_mutex;
std::vector<NodeInfo*> Floyd::nodes_info;
LeveldbBackend* Floyd::db;
raft::RaftConsensus* Floyd::raft_;

Floyd::Floyd(const Options& options)
  : options_(options) {

  db = new LeveldbBackend(options_.data_path);
  //meta_thread_ = new FloydMetaThread(options_.local_port);
  worker_thread_ = new FloydWorkerThread(options_.local_port);
  raft_ = new raft::RaftConsensus(options_);
}

Floyd::~Floyd() {
  //delete meta_thread_;
  delete worker_thread_;
  delete raft_;
  delete db;
}

bool Floyd::IsLeader() {
  std::pair<std::string, int> leader_node = raft_->GetLeaderNode();
  if (leader_node.first == options_.local_ip &&
      leader_node.second == options_.local_port) {
    return true;
  }
  return false;
}

bool Floyd::GetLeader(std::string& ip, int& port) {
  std::pair<std::string, int> leader_node = raft_->GetLeaderNode();
  if (leader_node.first == "" || leader_node.second == 0) {
    return false;
  }
  ip = leader_node.first;
  port = leader_node.second;
  return true;
}

void Floyd::GetAllNodes(std::vector<std::string> &nodes) {
  nodes_mutex.Lock();
  for (auto it = nodes_info.begin(); it != nodes_info.end(); it++) {
    nodes.push_back((*it)->ip + ":" + std::to_string((*it)->port));
  } 
  nodes_mutex.Unlock();
}

NodeInfo* Floyd::GetLeaderInfo() {
  std::pair<std::string, int> leader_node = raft_->GetLeaderNode();
  if (leader_node.first == "" || leader_node.second == 0) {
    return NULL;
  }
  NodeInfo* dummy_leader = new NodeInfo(leader_node.first, leader_node.second);
  std::function<bool(NodeInfo*)> is_leader = [&](NodeInfo * ni)->bool {
    if (*ni == *dummy_leader)
      return true;
    else
      return false;
  };
  nodes_mutex.Lock();
  std::vector<NodeInfo*>::iterator it =
      std::find_if(nodes_info.begin(), nodes_info.end(), is_leader);
  delete dummy_leader;
  if (it != nodes_info.end()) {
    nodes_mutex.Unlock();
    return *it;
  } else {
    nodes_mutex.Unlock();
    return NULL;
  }
}

Status Floyd::Delete(const std::string& key) {
  NodeInfo* leaderInfo = GetLeaderInfo();
  if (leaderInfo == NULL) {
    return Status::NotFound("no leader node!");
  }

  // Construct PB package
  command::Command cmd = BuildDeleteCommand(key);
  // Local node is leader?
  if (IsLeader()) {
    return raft_->HandleDeleteCommand(cmd);
  }
  command::CommandRes cmd_res;
  Rpc(leaderInfo, cmd, cmd_res);
  if (cmd_res.kvr().status())
    return Status::OK();
  else
    return Status::Corruption("delete error!");
}

Status Floyd::Write(const std::string& key, const std::string& value) {
  NodeInfo* leaderInfo = GetLeaderInfo();
  if (leaderInfo == NULL) {
    return Status::NotFound("no leader node!");
  }

  // Construct PB package
  command::Command cmd = BuildWriteCommand(key, value);
  // Local node is leader?
  if (IsLeader()) {
    return raft_->HandleWriteCommand(cmd);
  }
  command::CommandRes cmd_res;
  Rpc(leaderInfo, cmd, cmd_res);
  if (cmd_res.kvr().status())
    return Status::OK();
  else
    return Status::Corruption("write error!");
}

Status Floyd::Read(const std::string& key, std::string& value) {
  // Construct PB package
  NodeInfo* leaderInfo = GetLeaderInfo();
  if (leaderInfo == NULL) {
    return Status::NotFound("no leader node!");
  }

  command::Command cmd = BuildReadCommand(key);
  // Local node is leader?
  if (IsLeader()) {
    LOG_DEBUG("MainThread: Read as Leader");
    return raft_->HandleReadCommand(cmd, value);
  }

  command::CommandRes cmd_res;
  Rpc(leaderInfo, cmd, cmd_res);
  if (cmd_res.kvr().status()) {
    value = cmd_res.kvr().value();
    return Status::OK();
  } else {
    value = cmd_res.kvr().value();
    return Status::Corruption("leader read error!");
  }
}

Status Floyd::ReadAll(std::map<std::string, std::string>& kvMap) {
  // Construct PB package
  NodeInfo* leaderInfo = GetLeaderInfo();
  if (leaderInfo == NULL) {
    return Status::NotFound("no leader node!");
  }

  command::Command cmd = BuildReadAllCommand();

  // Local node is leader?
  if (IsLeader()) {
    return raft_->HandleReadAllCommand(cmd, kvMap);
  }

  command::CommandRes cmd_res;
  Rpc(leaderInfo, cmd, cmd_res);
  if (cmd_res.kvallr().status()) {
    for (auto it = cmd_res.kvallr().kvall().begin();
         it != cmd_res.kvallr().kvall().end(); ++it) {
      command::CommandRes_Kv kv = *it;
      kvMap.insert(std::make_pair(kv.key(), kv.value()));
    }
    return Status::OK();
  } else {
    return Status::Corruption("leader read error!");
  }
}

Status Floyd::DirtyRead(const std::string& key, std::string& value) {
  return db->Get(key, value);
}

Status Floyd::DirtyReadAll(std::map<std::string, std::string>& kvMap) {
  return db->GetAll(kvMap);
}

Status Floyd::DirtyWrite(const std::string& key, const std::string& value) {
  Status s = db->Set(key, value);
  if (!s.ok()) return s;

  MutexLock l(&nodes_mutex);
  std::vector<NodeInfo*>::iterator local_nis_iter = nodes_info.begin();
  command::Command cmd;
  cmd.set_type(command::Command::DirtyWrite);
  command::Command_Kv* kv = new command::Command_Kv();
  kv->set_key(key);
  kv->set_value(value);
  cmd.set_allocated_kv(kv);
  for (; local_nis_iter != nodes_info.end(); ++local_nis_iter) {
    if (((*local_nis_iter)->ip == options_.local_ip) &&
        ((*local_nis_iter)->port == options_.local_port))
      continue;
    // s = UpHoldWorkerCliConn(*local_nis_iter);
    s = (*local_nis_iter)->UpHoldWorkerCliConn();
    if (!s.ok()) continue;

    (*local_nis_iter)->dcc->SendMessage(&cmd);
  }
  return s;
}

Status Floyd::TryLock(const std::string& key) {
  // printf ("\nFloyd::TryLock key:%s\n", key.c_str());

  //   NodeInfo* leaderInfo = GetLeaderInfo();
  //   if (leaderInfo == NULL){
  //     return Status::NotFound("no leader node!");
  //   }
  std::pair<std::string, int> leader_node = raft_->GetLeaderNode();
  if (leader_node.first == "" || leader_node.second == 0) {
    return Status::NotFound("no leader node!");
  }

  // Construct Cmd PB package
  command::Command cmd;
  cmd.set_type(command::Command::TryLock);
  command::Command_Kv* kv = new command::Command_Kv();
  kv->set_key(key);
  cmd.set_allocated_kv(kv);

  command::Command_User* user = new command::Command_User();
  user->set_ip(options_.local_ip);
  user->set_port(options_.local_port);
  cmd.set_allocated_user(user);

  // Local node is leader?
  if (leader_node.first == options_.local_ip &&
      leader_node.second == options_.local_port) {
    // printf ("handle TryLock as leader\n");
    LOG_DEBUG("MainThread: TryLock as Leader");
    return raft_->HandleTryLockCommand(cmd);
  }

  // Redirect
  NodeInfo* dummy_leader = new NodeInfo(leader_node.first, leader_node.second);
  std::function<bool(NodeInfo*)> is_leader = [&](NodeInfo * ni)->bool {
    return (*ni) == (*dummy_leader);
  };

  nodes_mutex.Lock();
  std::vector<NodeInfo*>::iterator it =
      std::find_if(nodes_info.begin(), nodes_info.end(), is_leader);
  nodes_mutex.Unlock();
  delete dummy_leader;

  // printf ("TryLock redirect cmd to leader %s:%d\n", (*it)->ip.c_str(),
  // (*it)->port);
  LOG_DEBUG("MainThread: TryLock as Follower, redirect %s:%d",
            (*it)->ip.c_str(), (*it)->port);
  if (it != nodes_info.end()) {
    Status ret = (*it)->UpHoldWorkerCliConn();
    if (!ret.ok()) {
      return ret;
    }

    ret = (*it)->dcc->SendMessage(&cmd);
    if (!ret.ok()) {
      LOG_WARN("MainThread::TryLock as Follower, redirect:SendMeassge fail: %s",
               ret.ToString().c_str());
      (*it)->dcc->Close();
      delete (*it)->dcc;
      (*it)->dcc = NULL;
      return ret;
    }
    LOG_DEBUG("MainThread::TryLock as Follower, redirect:SendMeassge success");

    command::CommandRes cmd_res;
    ret = (*it)->dcc->GetResMessage(&cmd_res);
    if (!ret.ok()) {
      LOG_WARN(
          "MainThread::TryLock as Follower, redirect:GetResMessage fail: %s",
          ret.ToString().c_str());
      (*it)->dcc->Close();
      delete (*it)->dcc;
      (*it)->dcc = NULL;
      return ret;
    }
    LOG_DEBUG(
        "MainThread::TryLock as Follower, redirect:GetResMessage success");

    if (cmd_res.kvr().status()) {
      return Status::OK();
    } else {
      return Status::Corruption("TryLock error!");
    }
  } else {
    return Status::NotFound("no leader node!");
  }
}

Status Floyd::UnLock(const std::string& key) {
  // printf ("\nFloyd::UnLock key:%s\n", key.c_str());

  std::pair<std::string, int> leader_node = raft_->GetLeaderNode();
  if (leader_node.first == "" || leader_node.second == 0) {
    return Status::NotFound("no leader node!");
  }

  // Construct Cmd PB package
  command::Command cmd;
  cmd.set_type(command::Command::UnLock);
  command::Command_Kv* kv = new command::Command_Kv();
  kv->set_key(key);
  cmd.set_allocated_kv(kv);

  command::Command_User* user = new command::Command_User();
  user->set_ip(options_.local_ip);
  user->set_port(options_.local_port);
  cmd.set_allocated_user(user);

  // Local node is leader?
  if (leader_node.first == options_.local_ip &&
      leader_node.second == options_.local_port) {
    // printf ("handle UnLock as leader\n");
    return raft_->HandleUnLockCommand(cmd);
  }

  // Redirect
  NodeInfo* dummy_leader = new NodeInfo(leader_node.first, leader_node.second);
  std::function<bool(NodeInfo*)> is_leader = [&](NodeInfo * ni)->bool {
    return (*ni) == (*dummy_leader);
  };

  nodes_mutex.Lock();
  std::vector<NodeInfo*>::iterator it =
      std::find_if(nodes_info.begin(), nodes_info.end(), is_leader);
  nodes_mutex.Unlock();
  delete dummy_leader;

  // printf ("UnLock redirect cmd to leader %s:%d\n", (*it)->ip.c_str(),
  // (*it)->port);
  if (it != nodes_info.end()) {
    Status ret = (*it)->UpHoldWorkerCliConn();
    if (!ret.ok()) {
      return ret;
    }

    ret = (*it)->dcc->SendMessage(&cmd);
    if (!ret.ok()) {
      (*it)->dcc->Close();
      delete (*it)->dcc;
      (*it)->dcc = NULL;
      return ret;
    }

    command::CommandRes cmd_res;
    ret = (*it)->dcc->GetResMessage(&cmd_res);
    if (!ret.ok()) {
      // printf ("get reply error:%s\n", ret.ToString().c_str());
      (*it)->dcc->Close();
      delete (*it)->dcc;
      (*it)->dcc = NULL;
      return ret;
    }

    if (cmd_res.kvr().status()) {
      return Status::OK();
    } else {
      return Status::Corruption("UnLock error!");
    }
  } else {
    return Status::NotFound("no leader node!");
  }
}

Status Floyd::AddNodeFromMetaRes(meta::MetaRes* meta_res,
                                 std::vector<NodeInfo*>* nis) {
  MutexLock l(&Floyd::nodes_mutex);
  for (int i = 0; i < meta_res->nodes_size(); i++) {
    meta::MetaRes_Node node = meta_res->nodes(i);
    std::vector<NodeInfo*>::iterator iter = Floyd::nodes_info.begin();
    for (; iter != Floyd::nodes_info.end(); ++iter) {
      if (node.ip() == (*iter)->ip && node.port() == (*iter)->port) break;
    }
    if (iter == Floyd::nodes_info.end()) {
      NodeInfo* ni = new NodeInfo(node.ip(), node.port());
      nis->push_back(ni);
    }
  }
  return Status::OK();
}



Status Floyd::ChaseRaftLog(raft::RaftConsensus* raft_sensus) {
  command::Command_RaftStage* raftStage = new command::Command_RaftStage();
  raftStage->set_term(raft_sensus->GetCurrentTerm());
  raftStage->set_commitindex(raft_sensus->GetCurrentTerm());

  command::Command cmd;
  cmd.set_type(command::Command::SynRaftStage);
  cmd.set_allocated_raftstage(raftStage);
  
  // TODO anan ? not used
  int max_commit_index = 0;
  int max_term = 0;

  MutexLock l(&nodes_mutex);
  std::vector<NodeInfo*>::iterator iter = nodes_info.begin();
  for (; iter != nodes_info.end(); ++iter) {
    if (((*iter)->ip == options_.local_ip) &&
        ((*iter)->port == options_.local_port))
      continue;
    Status ret = (*iter)->UpHoldWorkerCliConn();
    if (!ret.ok()) continue;

    (*iter)->dcc->SendMessage(&cmd);

    command::CommandRes cmd_res;
    ret = (*iter)->dcc->GetResMessage(&cmd_res);
    max_commit_index =
        std::max(max_commit_index, cmd_res.raftstage().commitindex());
    max_term = std::max(max_term, cmd_res.raftstage().term());
  }

  raft_sensus->SetVoteCommitIndex(0);
  raft_sensus->SetVoteTerm(0);
  return Status::OK();
}

Status Floyd::Start() {
  LOG_DEBUG("MainThread::Start: floyd starting...");

  slash::CreatePath(options_.log_path);
  slash::CreatePath(options_.data_path);

  std::string ip;
  int port;
  for (auto it = options_.members.begin(); it != options_.members.end(); it++) {
    slash::ParseIpPortString(*it, ip, port);
    nodes_info.push_back(new NodeInfo(ip, port));

    //TODO(anan) wether to create and uphold
    //  remove node message
    //ni->UpHoldWorkerCliConn();
    //ni->mcc = new FloydMetaCliConn(ip, port);
  }

  Status result = db->Open();
  if (!result.ok()) {
    return result;
  }

  // Init should before WorkerThread in case of null log_
  raft_->Init();

  //TODO: anan check if threads start successfully
  // start heartbeat and meta thread
  //meta_thread_->StartThread();
  if (worker_thread_->StartThread() != 0) {
    LOG_ERROR("MainThread::Start: floyd worker thread failed to start");
  }
  ChaseRaftLog(raft_);

  LOG_DEBUG("MainThread::Start: floyd started");
  return Status::OK();
}

Status Floyd::Stop() {
  //delete meta_thread_;
  // delete heartbeat_;
  delete worker_thread_;
  delete db;
  delete raft_;
  std::vector<NodeInfo*>::iterator iter = nodes_info.begin();
  for (; iter != nodes_info.end(); ++iter) {
    if ((*iter)->mcc != NULL) {
      (*iter)->mcc->Close();
      delete (*iter)->mcc;
      (*iter)->mcc = NULL;
    }
    delete (*iter);
  }
  std::vector<NodeInfo*>().swap(nodes_info);
  return Status::OK();
}

Status Floyd::Erase() {
  Stop();
  std::string path = options_.data_path;
  if (path.back() != '/') path = path + '/';
  slash::DeleteDir(path);
  path = options_.log_path;
  if (path.back() != '/') path = path + '/';
  slash::DeleteDir(path);
  return Status::OK();
}
}
