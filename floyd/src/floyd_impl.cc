// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_impl.h"

#include <google/protobuf/text_format.h>

#include <utility>
#include <vector>

#include "slash/include/env.h"
#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"
#include "slash/include/slash_string.h"

#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_worker.h"
#include "floyd/src/raft_log.h"
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/floyd_primary_thread.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/logger.h"
#include "floyd/src/floyd.pb.h"

namespace floyd {

FloydImpl::FloydImpl(const Options& options)
  : options_(options),
    db_(NULL),
    info_log_(NULL) {
}

FloydImpl::~FloydImpl() {
  // worker will use floyd, delete worker first
  delete worker_;
  delete worker_client_pool_;
  delete peer_client_pool_;
  delete primary_;
  delete apply_;
  for (auto& pt : peers_) {
    delete pt.second;
  }

  delete context_;
  delete db_;
  delete raft_log_;
  delete info_log_;
}

bool FloydImpl::IsSelf(const std::string& ip_port) {
  return (ip_port ==
    slash::IpPortString(options_.local_ip, options_.local_port));
}

bool FloydImpl::GetLeader(std::string *ip_port) {
  std::string ip;
  int port;
  context_->leader_node(&ip, &port);
  if (ip.empty() || port == 0) {
    return false;
  }
  *ip_port = slash::IpPortString(ip, port);
  return true;
}

// TODO (baotiao): this function is wrong
bool FloydImpl::GetLeader(std::string* ip, int* port) {
  context_->leader_node(ip, port);
  return (!ip->empty() && *port != 0);
}

bool FloydImpl::HasLeader() {
  return context_->HasLeader();
}

bool FloydImpl::GetAllNodes(std::vector<std::string>& nodes) {
  nodes = options_.members;
  return true;
}

void FloydImpl::set_log_level(const int log_level) {
  if (info_log_) {
    info_log_->set_log_level(log_level);
  }
}

Status FloydImpl::Init() {
  slash::CreatePath(options_.path);
  if (NewLogger(options_.path + "/LOG", &info_log_) != 0) {
    return Status::Corruption("Open LOG failed, ", strerror(errno));
  }

  // TODO (anan) set timeout and retry
  peer_client_pool_ = new ClientPool(info_log_);
  worker_client_pool_ = new ClientPool(info_log_);

  // Create DB
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DB::Open(options, options_.path + "/db/", &db_);
  if (!s.ok()) {
    LOGV(ERROR_LEVEL, info_log_, "Open db failed! path: %s", options_.path.c_str());
    return Status::Corruption("Open DB failed, " + s.ToString());
  }

  // Recover Context
  raft_log_ = new RaftLog(options_.path + "/log/", info_log_);
  context_ = new FloydContext(options_, raft_log_, info_log_);
  context_->RecoverInit();

  // Create Apply threads
  apply_ = new FloydApply(context_, db_, raft_log_);

  // TODO(annan) peers and primary refer to each other
  // Create PrimaryThread before Peers
  primary_ = new FloydPrimary(context_, apply_);

  // Create peer threads
  for (auto iter = options_.members.begin();
      iter != options_.members.end(); iter++) {
    if (!IsSelf(*iter)) {
      Peer* pt = new Peer(*iter, context_, primary_, raft_log_, peer_client_pool_);
      peers_.insert(std::pair<std::string, Peer*>(*iter, pt));
    }
  }

  // Start peer thread
  int ret;
  for (auto& pt : peers_) {
    if ((ret = pt.second->StartThread()) != 0) {
      LOGV(ERROR_LEVEL, info_log_, "FloydImpl peer thread to %s failed to "
           " start, ret is %d", pt.first.c_str(), ret);
      return Status::Corruption("failed to start peer thread to " + pt.first);
    }
  }
  LOGV(INFO_LEVEL, info_log_, "Floyd start %d peer thread", peers_.size());

  // Start worker thread after Peers, because WorkerHandle will check peers
  worker_ = new FloydWorker(options_.local_port, 1000, this);
  if ((ret = worker_->Start()) != 0) {
    LOGV(ERROR_LEVEL, info_log_, "FloydImpl worker thread failed to start, ret is %d", ret);
    return Status::Corruption("failed to start worker, return " + std::to_string(ret));
  }

  // Set and Start PrimaryThread
  primary_->set_peers(&peers_);
  if ((ret = primary_->Start()) != 0) {
    LOGV(ERROR_LEVEL, info_log_, "FloydImpl primary thread failed to start, ret is %d", ret);
    return Status::Corruption("failed to start primary thread, return " + std::to_string(ret));
  }
  primary_->AddTask(kCheckElectLeader);

  // test only
  // options_.Dump();
  LOGV(INFO_LEVEL, info_log_, "Floyd started!\nOptions\n%s", options_.ToString().c_str());
  return Status::OK();
}

Status Floyd::Open(const Options& options, Floyd** floyd) {
  *floyd = NULL;
  Status s;
  FloydImpl *impl = new FloydImpl(options);
  s = impl->Init();
  if (s.ok()) {
    *floyd = impl;
  } else {
    delete impl;
  }
  return s;
}

Floyd::~Floyd() {
}

static void BuildReadRequest(const std::string& key, CmdRequest* cmd) {
  cmd->set_type(Type::kRead);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
}

static void BuildReadResponse(const std::string &key, const std::string &value,
                              StatusCode code, CmdResponse* response) {
  response->set_code(code);
  CmdResponse_Kv* kv = response->mutable_kv();
  if (code == StatusCode::kOk) {
    kv->set_value(value);
  }
}

static void BuildWriteRequest(const std::string& key,
                              const std::string& value, CmdRequest* cmd) {
  cmd->set_type(Type::kWrite);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
  kv->set_value(value);
}

static void BuildDirtyWriteRequest(const std::string& key,
                                   const std::string& value, CmdRequest* cmd) {
  cmd->set_type(Type::kDirtyWrite);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
  kv->set_value(value);
}

static void BuildDeleteRequest(const std::string& key, CmdRequest* cmd) {
  cmd->set_type(Type::kDelete);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
}

static void BuildRequestVoteResponse(uint64_t term, bool granted,
                                     CmdResponse* response) {
  response->set_type(Type::kRequestVote);
  CmdResponse_RequestVoteResponse* request_vote_res = response->mutable_request_vote_res();
  request_vote_res->set_term(term);
  request_vote_res->set_vote_granted(granted);
}

static void BuildAppendEntriesResponse(bool succ, uint64_t term,
                                       uint64_t log_index,
                                       CmdResponse* response) {
  response->set_type(Type::kAppendEntries);
  CmdResponse_AppendEntriesResponse* append_entries_res = response->mutable_append_entries_res();
  append_entries_res->set_term(term);
  append_entries_res->set_last_log_index(log_index);
  append_entries_res->set_success(succ);
}

static void BuildLogEntry(const CmdRequest& cmd, uint64_t current_term, Entry* entry) {
  entry->set_term(current_term);
  entry->set_key(cmd.kv().key());
  entry->set_value(cmd.kv().value());
  if (cmd.type() == Type::kRead) {
    entry->set_optype(Entry_OpType_kRead);
  } else if (cmd.type() == Type::kWrite || cmd.type() == Type::kDirtyWrite) {
    entry->set_optype(Entry_OpType_kWrite);
  } else if (cmd.type() == Type::kDelete) {
    entry->set_optype(Entry_OpType_kDelete);
  }
}

Status FloydImpl::Write(const std::string& key, const std::string& value) {
  if (!HasLeader()) {
    return Status::Incomplete("no leader node!");
  }
  CmdRequest cmd;
  BuildWriteRequest(key, value, &cmd);
  CmdResponse response;
  Status s = DoCommand(cmd, &response);
  if (!s.ok()) {
    return s;
  }
  if (response.code() == StatusCode::kOk) {
    return Status::OK();
  }
  return Status::Corruption("Write Error");
}

Status FloydImpl::DirtyWrite(const std::string& key, const std::string& value) {
  // Write myself first
  rocksdb::Status rs = db_->Put(rocksdb::WriteOptions(), key, value);
  if (!rs.ok()) {
    return Status::IOError("DirtyWrite failed, " + rs.ToString());
  }

  // Sync to other nodes without response
  CmdRequest cmd;
  BuildDirtyWriteRequest(key, value, &cmd);

  CmdResponse response;
  std::string local_server = slash::IpPortString(options_.local_ip, options_.local_port);
  for (auto& iter : options_.members) {
    if (iter != local_server) {
      Status s = worker_client_pool_->SendAndRecv(iter, cmd, &response);
      LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::DirtyWrite Send to %s return %s, key(%s) value(%s)",
           iter.c_str(), s.ToString().c_str(), cmd.kv().key().c_str(), cmd.kv().value().c_str());
    }
  }
  return Status::OK();
}

Status FloydImpl::Delete(const std::string& key) {
  if (!HasLeader()) {
    return Status::Incomplete("no leader node!");
  }
  CmdRequest cmd;
  BuildDeleteRequest(key, &cmd);

  CmdResponse response;
  Status s = DoCommand(cmd, &response);
  if (!s.ok()) {
    return s;
  }
  if (response.code() == StatusCode::kOk) {
    return Status::OK();
  }
  return Status::Corruption("Delete Error");
}

Status FloydImpl::Read(const std::string& key, std::string& value) {
  if (!HasLeader()) {
    return Status::Incomplete("no leader node!");
  }
  CmdRequest cmd;
  BuildReadRequest(key, &cmd);
  CmdResponse response;
  Status s = DoCommand(cmd, &response);
  if (!s.ok()) {
    return s;
  }
  if (response.code() == StatusCode::kOk) {
    value = response.kv().value();
    return Status::OK();
  } else if (response.code() == StatusCode::kNotFound) {
    return Status::NotFound("");
  } else {
    return Status::Corruption("Read Error");
  }
}

Status FloydImpl::DirtyRead(const std::string& key, std::string& value) {
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
  if (s.ok()) {
    return Status::OK();
  } else if (s.IsNotFound()) {
    return Status::NotFound("");
  }
  return Status::Corruption(s.ToString());
}

bool FloydImpl::GetServerStatus(std::string& msg) {
  LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::GetServerStatus start");

  CmdResponse_ServerStatus server_status;
  DoGetServerStatus(&server_status);

  char str[512];
  snprintf (str, 512,
            "      Node           | Role    |   Term    | CommitIdx |    Leader         |  VoteFor          | LastLogTerm | LastLogIdx | LastApplyIdx |\n" 
            "%15s:%-6d %9s %10lu %10lu %15s:%-6d %15s:%-6d %10lu %10lu %10lu\n",
            options_.local_ip.c_str(), options_.local_port,
            server_status.role().c_str(),
            server_status.term(), server_status.commit_index(),
            server_status.leader_ip().c_str(), server_status.leader_port(),
            server_status.voted_for_ip().c_str(), server_status.voted_for_port(),
            server_status.last_log_term(), server_status.last_log_index(),
            server_status.last_applied());

  msg.clear();
  msg.append(str);

  CmdRequest cmd;
  cmd.set_type(Type::kServerStatus);
  CmdResponse response;
  std::string local_server = slash::IpPortString(options_.local_ip, options_.local_port);
  for (auto& iter : options_.members) {
    if (iter != local_server) {
      Status s = worker_client_pool_->SendAndRecv(iter, cmd, &response);
      LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::GetServerStatus Send to %s return %s",
           iter.c_str(), s.ToString().c_str());
      if (s.ok()) {
        std::string ip;
        int port;
        slash::ParseIpPortString(iter, ip, port);
        CmdResponse_ServerStatus server_status = response.server_status();
        snprintf (str, 512,
                  "%15s:%-6d %9s %10lu %10lu %15s:%-6d %15s:%-6d %10lu %10lu %10lu\n",
                  ip.c_str(), port,
                  server_status.role().c_str(),
                  server_status.term(), server_status.commit_index(),
                  server_status.leader_ip().c_str(), server_status.leader_port(),
                  server_status.voted_for_ip().c_str(), server_status.voted_for_port(),
                  server_status.last_log_term(), server_status.last_log_index(),
                  server_status.last_applied());
        msg.append(str);
        LOGV(DEBUG_LEVEL, info_log_, "GetServerStatus msg(%s)", str);
      }
    }
  }
  return true;
}

Status FloydImpl::DoCommand(const CmdRequest& cmd, CmdResponse *response) {
  // Execute if is leader
  std::string leader_ip;
  int leader_port;
  context_->leader_node(&leader_ip, &leader_port);
  if (options_.local_ip == leader_ip && options_.local_port == leader_port) {
    return ExecuteCommand(cmd, response);
  }
  // Redirect to leader
  return worker_client_pool_->SendAndRecv(
      slash::IpPortString(leader_ip, leader_port),
      cmd, response);
}

Status FloydImpl::ReplyExecuteDirtyCommand(const CmdRequest& cmd,
                                      CmdResponse *response) {
  std::string value;
  rocksdb::Status rs;
  switch (cmd.type()) {
    case Type::kDirtyWrite: {
      rs = db_->Put(rocksdb::WriteOptions(), cmd.kv().key(), cmd.kv().value());
      //TODO(anan) add response type or reorganize proto
      //response->set_type(CmdResponse::DirtyWrite);
      response->set_type(Type::kWrite);
      CmdResponse_Kv* kv = response->mutable_kv();
      if (rs.ok()) {
        response->set_code(StatusCode::kOk);
      } else {
        response->set_code(StatusCode::kError);
      }

      LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ExecuteDirtyCommand DirtyWrite %s, key(%s) value(%s)",
           rs.ToString().c_str(), cmd.kv().key().c_str(), cmd.kv().value().c_str());
#ifndef NDEBUG
      std::string text_format;
      google::protobuf::TextFormat::PrintToString(*response, &text_format);
      LOGV(DEBUG_LEVEL, info_log_, "DirtyWrite Response :\n%s", text_format.c_str());
#endif
      break;
    }
    case Type::kServerStatus: {
      response->set_type(Type::kServerStatus);
      response->set_code(StatusCode::kOk);
      CmdResponse_ServerStatus* server_status = response->mutable_server_status();
      DoGetServerStatus(server_status);
      LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ExecuteDirtyCommand GetServerStatus");
      break;
    }
    default: {
      return Status::Corruption("Unknown cmd type");
    }
  }
  return Status::OK();
}

bool FloydImpl::DoGetServerStatus(CmdResponse_ServerStatus* res) {
  std::string role_msg;
  switch (context_->role()) {
    case Role::kFollower:
      role_msg = "follower";
      break;
    case Role::kCandidate:
      role_msg = "candidate";
      break;
    case Role::kLeader:
      role_msg = "leader";
      break;
  }

  res->set_term(context_->current_term());   
  res->set_commit_index(context_->commit_index());
  res->set_role(role_msg);

  std::string ip;
  int port;
  context_->leader_node(&ip, &port);
  if (ip.empty()) {
    res->set_leader_ip("null");
  } else {
    res->set_leader_ip(ip);
  }
  res->set_leader_port(port);

  context_->voted_for_node(&ip, &port);
  if (ip.empty()) {
    res->set_voted_for_ip("null");
  } else {
    res->set_voted_for_ip(ip);
  }
  res->set_voted_for_port(port);

  uint64_t last_log_index;
  uint64_t last_log_term;
  raft_log_->GetLastLogTermAndIndex(&last_log_term, &last_log_index);

  res->set_last_log_term(last_log_term);
  res->set_last_log_index(last_log_index);
  res->set_last_applied(context_->last_applied());
  return true;
}

Status FloydImpl::ExecuteCommand(const CmdRequest& cmd,
                                 CmdResponse *response) {
  // Append entry local
  std::vector<Entry*> entries;
  Entry entry;
  BuildLogEntry(cmd, context_->current_term(), &entry);
  entries.push_back(&entry);

  uint64_t last_log_index = raft_log_->Append(entries);
  if (last_log_index <= 0) {
    return Status::IOError("Append Entry failed");
  }

  // Notify primary then wait for apply
  if (options_.single_mode) {
    primary_->AddTask(kAdvanceCommitIndex);
  } else {
    primary_->AddTask(kNewCommand);
  }

  response->set_type(cmd.type());
  response->set_code(StatusCode::kError);

  Status res = context_->WaitApply(last_log_index, 1000);
  if (!res.ok()) {
    return res;
  } 

  // Complete CmdRequest if needed
  std::string value;
  rocksdb::Status rs;
  switch (cmd.type()) {
  case Type::kWrite: {
    response->set_code(StatusCode::kOk);
    break;
  }
  case Type::kDelete: {
    response->set_code(StatusCode::kOk);
    break;
  }
  case Type::kRead: {
    rs = db_->Get(rocksdb::ReadOptions(), cmd.kv().key(), &value);
    if (rs.ok()) {
      BuildReadResponse(cmd.kv().key(), value, StatusCode::kOk, response);
    } else if (rs.IsNotFound()) {
      BuildReadResponse(cmd.kv().key(), value, StatusCode::kNotFound, response);
    } else {
      BuildReadResponse(cmd.kv().key(), value, StatusCode::kError, response);
    }
    LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ExecuteCommand Read %s, key(%s) value(%s)",
         rs.ToString().c_str(), cmd.kv().key().c_str(), value.c_str());
#ifndef NDEBUG 
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(*response, &text_format);
    LOGV(DEBUG_LEVEL, info_log_, "ReadResponse :\n%s", text_format.c_str());
#endif
    break;
  }
  default: {
    return Status::Corruption("Unknown cmd type");
  }
  }
  return Status::OK();
}

void FloydImpl::ReplyRequestVote(const CmdRequest& cmd, CmdResponse* response) {
  bool granted = false;
  CmdRequest_RequestVote request_vote = cmd.request_vote();
  LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ReplyRequestVote: my_term=%lu rqv.term=%lu",
       my_term, request_vote.term());
  MutexLock l(context_->commit_mu_);
  uint64_t my_current_term = context_->current_term();
  uint64_t my_last_log_index = context_->last_log_index();
  context_->raft_log_->GetLastLogTermAndIndex(&my_log_term, &my_log_index);
  // if caller's term smaller than my term, then I will notice him
  if (request_vote.last_log_term() < my_current_term) {
    BuildRequestVoteResponse(my_current_term, granted, response);
    return;
  }

  // if votedfor is null or candidateId, and candidated's log is at least as up-to-date
  // as receiver's log, grant vote
  if (!voted_for_ip_.empty() && (voted_for_ip_ != ip || voted_for_port_ != port) 
      (request_vote.last_log_term() == my_current_term && request_vote.last_log_index() >= my_last_log_index)) {
    
    LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ReplyRequestVote: BecomeFollower with current_term_(%lu) and new_term(%lu)"
        " commit_index(%lu)  last_applied(%lu)",
        my_current_term, request_vote.last_log_term(), my_last_log_index, ());
    context_->BecomeFollower(request_vote.term());
    primary_->ResetElectLeaderTimer();
    BuildRequestVoteResponse(my_current_term, granted, response);
    return ;
  }

  // Got my vote
  voted_for_ip_ = ip;
  voted_for_port_ = port;
  *my_term = current_term_;
  MetaApply();
  LOGV(INFO_LEVEL, info_log_, "FloydContext::RequestVote: grant vote for (%s:%d),"
       " with my_term(%lu), my last_log(%lu:%lu), caller log(%lu,%lu).",
       voted_for_ip_.c_str(), voted_for_port_, *my_term,
       my_log_term, my_log_index, log_term, log_index);
  return true;
  BuildRequestVoteResponse(my_term, granted, response);
}

void FloydImpl::ReplyAppendEntries(CmdRequest& cmd, CmdResponse* response) {
  // Ignore stale term
  MutexLock l(context_->commit_mu_);
  bool status = false;
  uint64_t my_term = context_->current_term;
  CmdRequest_AppendEntries append_entries = cmd.append_entries();
  // if the append entries term is smaller then my_term, then the caller must an older leader
  if (append_entries.term() < my_term) {
    BuildAppendEntriesResponse(status, my_term, raft_log_->GetLastLogIndex(), response);
    return;
  }
  // TODO(ba0tiao) why we need become follower, maybe we have been follower before
  context_->BecomeFollower(append_entries.term(),
                           append_entries.ip(), append_entries.port());

  std::vector<Entry*> entries;
  for (auto& it : *(cmd.mutable_append_entries()->mutable_entries())) {
    entries.push_back(&it);
  }
  // TODO(ba0tiao) do consistency check here

  /*
   * std::string text_format;
   * google::protobuf::TextFormat::PrintToString(cmd, &text_format);
   * LOGV(DEBUG_LEVEL, context_->info_log(), "FloydImpl::ReplyAppendEntries with %llu "
   *      "entries, message :\n%s",
   *      entries.size(), text_format.c_str());
   */

  // Append entries
  status = context_->ReceiverDoAppendEntries(append_entries.term(),
                                   append_entries.prev_log_term(),
                                   append_entries.prev_log_index(),
                                   entries, &my_term);

  // only when follower successfully do appendentries, we will update commit index
  if (status) {
    context_->AdvanceFollowerCommitIndex(append_entries.leader_commit());
    LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries after AdvanceCommitIndex %lu",
        context_->commit_index());
    apply_->ScheduleApply();
  }

  // TODO(anan) ElectLeader timer may timeout because of slow AppendEntries
  //   we delay reset timer.
  primary_->ResetElectLeaderTimer();
  BuildAppendEntriesResponse(status, my_term, raft_log_->GetLastLogIndex(), response);
}

} // namespace floyd
