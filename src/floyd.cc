#include "include/slash_status.h"
#include "include/floyd.h"
#include "src/meta.pb.h"
#include "src/command.pb.h"
#include "include/env.h"
#include "include/logger.h"
namespace floyd {

Mutex Floyd::nodes_mutex;
std::vector<NodeInfo*> Floyd::nodes_info;
DbBackend* Floyd::db;
floyd::raft::RaftConsensus* Floyd::raft_con;

Floyd::Floyd(const Options& options) : options_(options) {
  //@todo: When exists data
  if (options_.storage_type == "leveldb")
    db = new LeveldbBackend(options_.data_path);
  floydmeta_ = new FloydMetaThread(options_.local_port);
  // heartbeat_ = new FloydHeartBeatThread(options_.local_ip,
  // options_.local_port);
  floydworker_ = new FloydWorkerThread(options_.local_port + 100);
  raft_con = new raft::RaftConsensus(options);
}

Floyd::~Floyd() {
  delete floydmeta_;
  delete floydworker_;
  delete raft_con;
  delete db;
}

bool Floyd::IsLeader() {
  std::pair<std::string, int> leader_node = raft_con->GetLeaderNode();
  if (leader_node.first == options_.local_ip &&
      leader_node.second == options_.local_port) {
    return true;
  }
  return false;
}

bool Floyd::GetLeader(std::string& ip, int& port) {
  std::pair<std::string, int> leader_node = raft_con->GetLeaderNode();
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
  std::pair<std::string, int> leader_node = raft_con->GetLeaderNode();
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
    return raft_con->HandleDeleteCommand(cmd);
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
    return raft_con->HandleWriteCommand(cmd);
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
    return raft_con->HandleReadCommand(cmd, value);
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
    return raft_con->HandleReadAllCommand(cmd, kvMap);
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

    (*local_nis_iter)->dcc->Send(&cmd);
  }
  return s;
}

Status Floyd::TryLock(const std::string& key) {
  // printf ("\nFloyd::TryLock key:%s\n", key.c_str());

  //   NodeInfo* leaderInfo = GetLeaderInfo();
  //   if (leaderInfo == NULL){
  //     return Status::NotFound("no leader node!");
  //   }
  std::pair<std::string, int> leader_node = raft_con->GetLeaderNode();
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
    return raft_con->HandleTryLockCommand(cmd);
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

    ret = (*it)->dcc->Send(&cmd);
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
    ret = (*it)->dcc->Recv(&cmd_res);
    if (!ret.ok()) {
      LOG_WARN(
          "MainThread::TryLock as Follower, redirect:Recv fail: %s",
          ret.ToString().c_str());
      (*it)->dcc->Close();
      delete (*it)->dcc;
      (*it)->dcc = NULL;
      return ret;
    }
    LOG_DEBUG(
        "MainThread::TryLock as Follower, redirect:Recv success");

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

  std::pair<std::string, int> leader_node = raft_con->GetLeaderNode();
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
    return raft_con->HandleUnLockCommand(cmd);
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

    ret = (*it)->dcc->Send(&cmd);
    if (!ret.ok()) {
      (*it)->dcc->Close();
      delete (*it)->dcc;
      (*it)->dcc = NULL;
      return ret;
    }

    command::CommandRes cmd_res;
    ret = (*it)->dcc->Recv(&cmd_res);
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

/* fetch node map from a remote node
*/
Status Floyd::FetchRemoteMap(const std::string& ip, const int port,
                             std::vector<NodeInfo*>* nis) {
  Status ret;
  NodeInfo* ni = new NodeInfo(ip, port);
  ni->mcc = pink::NewPbCli();
  // todo change status
  ret = ni->mcc->Connect(ip, port);
  if (ret.ok()) {
    meta::Meta meta;
    meta.set_t(meta::Meta::NODE);
    std::vector<NodeInfo*>::iterator iter = Floyd::nodes_info.begin();
    for (; iter != Floyd::nodes_info.end(); ++iter) {
      meta::Meta_Node* node = meta.add_nodes();
      node->set_ip((*iter)->ip);
      node->set_port((*iter)->port);
    }
    // send request
    ret = ni->mcc->Send(&meta);
    if (!ret.ok()) {
      ni->mcc->Close();
      delete ni->mcc;
      ni->mcc = NULL;
      return ret;
    }

    meta::MetaRes meta_res;
    ret = ni->mcc->Recv(&meta_res);
    if (!ret.ok()) {
      ni->mcc->Close();
      delete ni->mcc;
      ni->mcc = NULL;
      return ret;
    }

    AddNodeFromMetaRes(&meta_res, nis);
  }

  if (!ret.ok()) {
    return ret;
  }
  return Status::OK();
}

/* Merge cluster map from remote node recursively , in a depth-first-serach way
 *
 * 1. fetch remote node's map A
 * 2. find strange nodes from map A,add it to self
 * 3. merge their map too
 *
 */
Status Floyd::MergeRemoteMap(const std::string& ip, const int port) {
  Status ret = Status::OK();
  std::vector<NodeInfo*> remote_nis;
  ret = FetchRemoteMap(ip, port, &remote_nis);
  if (!ret.ok()) {
    return ret;
  }

  std::vector<NodeInfo*>::iterator remote_nis_iter = remote_nis.begin();
  for (; remote_nis_iter != remote_nis.end(); ++remote_nis_iter) {
    bool isknown = false;
    std::vector<NodeInfo*>::iterator local_nis_iter = nodes_info.begin();
    for (; local_nis_iter != nodes_info.end(); ++local_nis_iter) {
      if (**local_nis_iter == **remote_nis_iter) {
        isknown = true;
        break;
      }
    }
    if (isknown) {
      continue;
    }
    NodeInfo* ni =
        new NodeInfo((*remote_nis_iter)->ip, (*remote_nis_iter)->port);
    ni->UpHoldWorkerCliConn();
    // UpHoldWorkerCliConn(ni);
    nodes_info.push_back(ni);
    LOG_DEBUG("MainThread::MergeRemoteMap: add new node %s:%d", ni->ip.c_str(),
              ni->port);
    if ((*remote_nis_iter)->ip != ip || (*remote_nis_iter)->port != port)
      ret = MergeRemoteMap((*remote_nis_iter)->ip, (*remote_nis_iter)->port);
  }
  return Status::OK();
}

Status Floyd::ChaseRaftLog(raft::RaftConsensus* raft_consensus) {
  MutexLock l(&nodes_mutex);
  std::vector<NodeInfo*>::iterator local_nis_iter = nodes_info.begin();
  command::Command cmd;
  command::Command_RaftStage* raftStage = new command::Command_RaftStage();
  raftStage->set_term(raft_consensus->GetCurrentTerm());
  raftStage->set_commitindex(raft_consensus->GetCurrentTerm());
  cmd.set_type(command::Command::SynRaftStage);
  cmd.set_allocated_raftstage(raftStage);
  int max_commit_index = 0;
  int max_term = 0;
  for (; local_nis_iter != nodes_info.end(); ++local_nis_iter) {
    if (((*local_nis_iter)->ip == options_.local_ip) &&
        ((*local_nis_iter)->port == options_.local_port))
      continue;
    Status ret = (*local_nis_iter)->UpHoldWorkerCliConn();
    if (!ret.ok()) continue;

    (*local_nis_iter)->dcc->Send(&cmd);

    command::CommandRes cmd_res;
    ret = (*local_nis_iter)->dcc->Recv(&cmd_res);
    max_commit_index =
        std::max(max_commit_index, cmd_res.raftstage().commitindex());
    max_term = std::max(max_term, cmd_res.raftstage().term());
  }

  raft_consensus->SetVoteCommitIndex(0);
  raft_consensus->SetVoteTerm(0);
  return Status::OK();
}

Status Floyd::Start() {
  LOG_DEBUG("MainThread::Start: floyd starting...");
  NodeInfo* ni = new NodeInfo(options_.local_ip, options_.local_port);
  nodes_info.push_back(ni);
  Status ret = Status::OK();
  // join myself
  if (options_.seed_ip != options_.local_ip ||
      options_.seed_port != options_.local_port) {
    ret = MergeRemoteMap(options_.seed_ip, options_.seed_port);
    if (!ret.ok()) {
      return ret;
    }
  }

  // start heartbeat and meta thread
  ret = db->Open();
  if (!ret.ok()) {
    return ret;
  }

  // Init should before WorkerThread in case of null log_
  raft_con->Init();

  //@TODO check if threads start successfully
  floydmeta_->StartThread();
  // heartbeat_->StartThread();
  floydworker_->StartThread();
  ChaseRaftLog(raft_con);

  LOG_DEBUG("MainThread::Start: floyd started");
  return Status::OK();
}

Status Floyd::SingleStart() {
  LOG_DEBUG("MainThread::SingleStart: floyd starting...");
  NodeInfo* ni = new NodeInfo(options_.local_ip, options_.local_port);
  nodes_info.push_back(ni);
  Status ret = Status::OK();
  ret = db->Open();
  if (!ret.ok()) {
    return ret;
  }

  // Init should before WorkerThread in case of null log_
  raft_con->InitAsLeader();

  //@TODO check if threads start successfully
  floydmeta_->StartThread();
  // heartbeat_->StartThread();
  floydworker_->StartThread();
  LOG_DEBUG("MainThread::SingleStart: floyd started");
  return Status::OK();
}

Status Floyd::Stop() {
  delete floydmeta_;
  // delete heartbeat_;
  delete floydworker_;
  delete db;
  delete raft_con;
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
