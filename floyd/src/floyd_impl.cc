// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_impl.h"

#include <google/protobuf/text_format.h>

#include <utility>
#include <vector>
#include <algorithm>

#include "pink/include/bg_thread.h"
#include "slash/include/env.h"
#include "slash/include/slash_string.h"
#include "slash/include/slash_mutex.h"

#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_worker.h"
#include "floyd/src/raft_log.h"
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/floyd_primary_thread.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/logger.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/raft_meta.h"

namespace floyd {

FloydImpl::FloydImpl(const Options& options)
  : db_(NULL),
    log_and_meta_(NULL),
    options_(options),
    info_log_(NULL) {
}

FloydImpl::~FloydImpl() {
  // worker will use floyd, delete worker first
  worker_->Stop();
  primary_->Stop();
  apply_->Stop();
  delete worker_;
  delete worker_client_pool_;
  delete primary_;
  delete apply_;
  for (auto& pt : peers_) {
    pt.second->Stop();
    delete pt.second;
  }
  delete context_;
  delete raft_meta_;
  delete raft_log_;
  delete info_log_;
  delete db_;
  delete log_and_meta_;
}

bool FloydImpl::IsSelf(const std::string& ip_port) {
  return (ip_port == slash::IpPortString(options_.local_ip, options_.local_port));
}

bool FloydImpl::GetLeader(std::string *ip_port) {
  if (context_->leader_ip.empty() || context_->leader_port == 0) {
    return false;
  }
  *ip_port = slash::IpPortString(context_->leader_ip, context_->leader_port);
  return true;
}

bool FloydImpl::IsLeader() {
  if (context_->leader_ip == "" || context_->leader_port == 0) {
    return false;
  }
  if (context_->leader_ip == options_.local_ip && context_->leader_port == options_.local_port) {
    return true;
  }
  return false;
}

bool FloydImpl::GetLeader(std::string* ip, int* port) {
  *ip = context_->leader_ip;
  *port = context_->leader_port;
  return (!ip->empty() && *port != 0);
}

bool FloydImpl::HasLeader() {
  if (context_->leader_ip == "" || context_->leader_port == 0) {
    return false;
  }
  return true;
}

bool FloydImpl::GetAllNodes(std::vector<std::string>* nodes) {
  *nodes = options_.members;
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

  // TODO(anan) set timeout and retry
  worker_client_pool_ = new ClientPool(info_log_);

  // Create DB
  rocksdb::Options options;
  options.create_if_missing = true;
  // options.disableDataSync = true;
  options.write_buffer_size = 1024 * 1024 * 1024;
  options.max_background_flushes = 8;
  rocksdb::Status s = rocksdb::DB::Open(options, options_.path + "/db/", &db_);
  if (!s.ok()) {
    LOGV(ERROR_LEVEL, info_log_, "Open db failed! path: %s", options_.path.c_str());
    return Status::Corruption("Open DB failed, " + s.ToString());
  }

  s = rocksdb::DB::Open(options, options_.path + "/log/", &log_and_meta_);
  if (!s.ok()) {
    LOGV(ERROR_LEVEL, info_log_, "Open DB log_and_meta failed! path: %s", options_.path.c_str());
    return Status::Corruption("Open DB log_and_meta failed, " + s.ToString());
  }

  // Recover Context
  raft_log_ = new RaftLog(log_and_meta_, info_log_);
  raft_meta_ = new RaftMeta(log_and_meta_, info_log_);
  raft_meta_->Init();
  context_ = new FloydContext(options_);
  context_->RecoverInit(raft_meta_);

  // Create Apply threads
  apply_ = new FloydApply(context_, db_, raft_meta_, raft_log_, info_log_);
  apply_->Start();

  // peers and primary refer to each other
  // Create PrimaryThread before Peers
  primary_ = new FloydPrimary(context_, raft_meta_, options_, info_log_);

  // Create peer threads
  // peers_.clear();
  for (auto iter = options_.members.begin(); iter != options_.members.end(); iter++) {
    if (!IsSelf(*iter)) {
      Peer* pt = new Peer(*iter, context_, primary_, raft_meta_, raft_log_,
          worker_client_pool_, apply_, options_, info_log_);
      peers_.insert(std::pair<std::string, Peer*>(*iter, pt));
    }
  }

  // Start peer thread
  int ret;
  for (auto& pt : peers_) {
    pt.second->set_peers(peers_);
    if ((ret = pt.second->Start()) != 0) {
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
  primary_->set_peers(peers_);
  if ((ret = primary_->Start()) != 0) {
    LOGV(ERROR_LEVEL, info_log_, "FloydImpl primary thread failed to start, ret is %d", ret);
    return Status::Corruption("failed to start primary thread, return " + std::to_string(ret));
  }
  primary_->AddTask(kCheckLeader);

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
  CmdRequest_KvRequest* kv_request = cmd->mutable_kv_request();
  kv_request->set_key(key);
}

static void BuildReadResponse(const std::string &key, const std::string &value,
                              StatusCode code, CmdResponse* response) {
  response->set_code(code);
  CmdResponse_KvResponse* kv_response = response->mutable_kv_response();
  if (code == StatusCode::kOk) {
    kv_response->set_value(value);
  }
}

static void BuildWriteRequest(const std::string& key,
                              const std::string& value, CmdRequest* cmd) {
  cmd->set_type(Type::kWrite);
  CmdRequest_KvRequest* kv_request = cmd->mutable_kv_request();
  kv_request->set_key(key);
  kv_request->set_value(value);
}

static void BuildDeleteRequest(const std::string& key, CmdRequest* cmd) {
  cmd->set_type(Type::kDelete);
  CmdRequest_KvRequest* kv_request = cmd->mutable_kv_request();
  kv_request->set_key(key);
}

static void BuildTryLockRequest(const std::string& name, const std::string& holder, uint64_t ttl,
                              CmdRequest* cmd) {
  cmd->set_type(Type::kTryLock);
  CmdRequest_LockRequest* lock_request = cmd->mutable_lock_request();
  lock_request->set_name(name);
  lock_request->set_holder(holder);
  lock_request->set_lease_end(slash::NowMicros() + ttl * 1000);
}

static void BuildUnLockRequest(const std::string& name, const std::string& holder,
                              CmdRequest* cmd) {
  cmd->set_type(Type::kUnLock);
  CmdRequest_LockRequest* lock_request = cmd->mutable_lock_request();
  lock_request->set_name(name);
  lock_request->set_holder(holder);
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
  entry->set_key(cmd.kv_request().key());
  entry->set_value(cmd.kv_request().value());
  if (cmd.type() == Type::kRead) {
    entry->set_optype(Entry_OpType_kRead);
  } else if (cmd.type() == Type::kWrite) {
    entry->set_optype(Entry_OpType_kWrite);
  } else if (cmd.type() == Type::kDelete) {
    entry->set_optype(Entry_OpType_kDelete);
  } else if (cmd.type() == Type::kTryLock) {
    entry->set_optype(Entry_OpType_kTryLock);
    entry->set_key(cmd.lock_request().name());
    entry->set_holder(cmd.lock_request().holder());
    entry->set_lease_end(cmd.lock_request().lease_end());
  } else if (cmd.type() == Type::kUnLock) {
    entry->set_optype(Entry_OpType_kUnLock);
    entry->set_key(cmd.lock_request().name());
    entry->set_holder(cmd.lock_request().holder());
  }
}

Status FloydImpl::Write(const std::string& key, const std::string& value) {
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

Status FloydImpl::Delete(const std::string& key) {
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

Status FloydImpl::Read(const std::string& key, std::string* value) {
  CmdRequest request;
  BuildReadRequest(key, &request);
  CmdResponse response;
  Status s = DoCommand(request, &response);
  if (!s.ok()) {
    return s;
  }
  if (response.code() == StatusCode::kOk) {
    *value = response.kv_response().value();
    return Status::OK();
  } else if (response.code() == StatusCode::kNotFound) {
    return Status::NotFound("not found the key");
  } else {
    return Status::Corruption("Read Error");
  }
}

Status FloydImpl::DirtyRead(const std::string& key, std::string* value) {
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, value);
  if (s.ok()) {
    return Status::OK();
  } else if (s.IsNotFound()) {
    return Status::NotFound("");
  }
  return Status::Corruption(s.ToString());
}

Status FloydImpl::TryLock(const std::string& name, const std::string& holder, uint64_t ttl) {
  CmdRequest request;
  BuildTryLockRequest(name, holder, ttl, &request);
  CmdResponse response;
  Status s = DoCommand(request, &response);
  if (!s.ok()) {
    return s;
  }
  if (response.code() == StatusCode::kOk) {
    return Status::OK();
  }
  return Status::Corruption("Lock Error");
}

Status FloydImpl::UnLock(const std::string& name, const std::string& holder) {
  CmdRequest request;
  BuildUnLockRequest(name, holder, &request);
  CmdResponse response;
  Status s = DoCommand(request, &response);
  if (!s.ok()) {
    return s;
  }
  if (response.code() == StatusCode::kOk) {
    return Status::OK();
  }
  return Status::Corruption("UnLock Error");
}

bool FloydImpl::GetServerStatus(std::string* msg) {
  LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::GetServerStatus start");
  slash::MutexLock l(&context_->global_mu);

  CmdResponse_ServerStatus server_status;
  DoGetServerStatus(&server_status);

  char str[512];
  snprintf (str, sizeof(str),
            "      Node           |    Role    | Term |      Leader      |      VoteFor      | LastLogTerm | LastLogIdx | CommitIndex | LastApplied |\n"
            "%15s:%-6d%10s%7lu%14s:%-6d%14s:%-6d%10lu%13lu%14lu%13lu\n",
            options_.local_ip.c_str(), options_.local_port, server_status.role().c_str(), server_status.term(),
            server_status.leader_ip().c_str(), server_status.leader_port(),
            server_status.voted_for_ip().c_str(), server_status.voted_for_port(),
            server_status.last_log_term(), server_status.last_log_index(), server_status.commit_index(),
            server_status.last_applied());

  msg->clear();
  msg->append(str);

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
        snprintf (str, sizeof(str),
                  "%15s:%-6d%10s%7lu%14s:%-6d%14s:%-6d%10lu%13lu%14lu%13lu\n",
                  ip.c_str(), port, server_status.role().c_str(), server_status.term(),
                  server_status.leader_ip().c_str(), server_status.leader_port(),
                  server_status.voted_for_ip().c_str(), server_status.voted_for_port(),
                  server_status.last_log_term(), server_status.last_log_index(), server_status.commit_index(),
                  server_status.last_applied());
        msg->append(str);
      }
    }
  }
  return true;
}

Status FloydImpl::DoCommand(const CmdRequest& request, CmdResponse *response) {
  // Execute if is leader
  std::string leader_ip;
  int leader_port;
  {
  slash::MutexLock l(&context_->global_mu);
  leader_ip = context_->leader_ip;
  leader_port = context_->leader_port;
  }
  if (options_.local_ip == leader_ip && options_.local_port == leader_port) {
    return ExecuteCommand(request, response);
  } else if (leader_ip == "" || leader_port == 0) {
    return Status::Incomplete("no leader node!");
  }
  // Redirect to leader
  return worker_client_pool_->SendAndRecv(
      slash::IpPortString(leader_ip, leader_port),
      request, response);
}

Status FloydImpl::ReplyExecuteDirtyCommand(const CmdRequest& cmd,
                                      CmdResponse *response) {
  std::string value;
  rocksdb::Status rs;
  switch (cmd.type()) {
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
  switch (context_->role) {
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

  res->set_term(context_->current_term);
  res->set_commit_index(context_->commit_index);
  res->set_role(role_msg);

  std::string ip;
  int port;
  ip = context_->leader_ip;
  port = context_->leader_port;
  if (ip.empty()) {
    res->set_leader_ip("null");
  } else {
    res->set_leader_ip(ip);
  }
  res->set_leader_port(port);

  ip = context_->voted_for_ip;
  port = context_->voted_for_port;
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
  res->set_last_applied(raft_meta_->GetLastApplied());
  return true;
}

Status FloydImpl::ExecuteCommand(const CmdRequest& request,
                                 CmdResponse *response) {
  // Append entry local
  std::vector<const Entry*> entries;
  Entry entry;
  BuildLogEntry(request, context_->current_term, &entry);
  entries.push_back(&entry);
  response->set_type(request.type());
  response->set_code(StatusCode::kError);

  uint64_t last_log_index = raft_log_->Append(entries);
  if (last_log_index <= 0) {
    return Status::IOError("Append Entry failed");
  }

  // Notify primary then wait for apply
  if (options_.single_mode) {
    context_->commit_index = last_log_index;
    raft_meta_->SetCommitIndex(context_->commit_index);
    apply_->ScheduleApply();
  } else {
    primary_->AddTask(kNewCommand);
  }

  {
  slash::MutexLock l(&context_->apply_mu);
  while (context_->last_applied < last_log_index) {
    if (!context_->apply_cond.TimedWait(1000)) {
      return Status::Timeout("FloydImpl::ExecuteCommand Timeout");
    }
  }
  }

  // Complete CmdRequest if needed
  std::string value;
  rocksdb::Status rs;
  Lock lock;
  switch (request.type()) {
    case Type::kWrite:
      response->set_code(StatusCode::kOk);
      break;
    case Type::kDelete:
      response->set_code(StatusCode::kOk);
      break;
    case Type::kRead:
      rs = db_->Get(rocksdb::ReadOptions(), request.kv_request().key(), &value);
      if (rs.ok()) {
        BuildReadResponse(request.kv_request().key(), value, StatusCode::kOk, response);
      } else if (rs.IsNotFound()) {
        BuildReadResponse(request.kv_request().key(), value, StatusCode::kNotFound, response);
      } else {
        BuildReadResponse(request.kv_request().key(), value, StatusCode::kError, response);
        return Status::Corruption("get key error");
      }
      LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ExecuteCommand Read %s, key(%s) value(%s)",
           rs.ToString().c_str(), request.kv_request().key().c_str(), value.c_str());
      break;
    case Type::kTryLock:
      rs = db_->Get(rocksdb::ReadOptions(), request.lock_request().name(), &value);
      if (rs.ok()) {
        lock.ParseFromString(value);
        if (lock.holder() == request.lock_request().holder() && lock.lease_end() == request.lock_request().lease_end()) {
          response->set_code(StatusCode::kOk);
        }
      } else {
        response->set_code(StatusCode::kLocked);
      }
      break;
    case Type::kUnLock:
      rs = db_->Get(rocksdb::ReadOptions(), request.lock_request().name(), &value);
      if (rs.IsNotFound()) {
        response->set_code(StatusCode::kOk);
      } else {
        response->set_code(StatusCode::kLocked);
      }
      break;
    default:
      return Status::Corruption("Unknown request type");
  }
  return Status::OK();
}

// Peer ask my vote with it's ip, port, log_term and log_index
void FloydImpl::GrantVote(uint64_t term, const std::string ip, int port) {
  // Got my vote
  context_->voted_for_ip = ip;
  context_->voted_for_port = port;
  context_->current_term = term;
}

int FloydImpl::ReplyRequestVote(const CmdRequest& request, CmdResponse* response) {
  slash::MutexLock l(&context_->global_mu);
  bool granted = false;
  CmdRequest_RequestVote request_vote = request.request_vote();
  LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ReplyRequestVote: my_term=%lu request.term=%lu",
       context_->current_term, request_vote.term());
  /*
   * If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
   */
  if (request_vote.term() > context_->current_term) {
    context_->BecomeFollower(request_vote.term());
    raft_meta_->SetCurrentTerm(context_->current_term);
  }
  // if caller's term smaller than my term, then I will notice him
  if (request_vote.term() < context_->current_term) {
    LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyRequestVote: Leader %s:%d term %lu is smaller than my %s:%d current term %lu",
        request_vote.ip().c_str(), request_vote.port(), request_vote.term(), options_.local_ip.c_str(), options_.local_port,
        context_->current_term);
    BuildRequestVoteResponse(context_->current_term, granted, response);
    return -1;
  }
  uint64_t my_last_log_term = 0;
  uint64_t my_last_log_index = 0;
  raft_log_->GetLastLogTermAndIndex(&my_last_log_term, &my_last_log_index);
  // if votedfor is null or candidateId, and candidated's log is at least as up-to-date
  // as receiver's log, grant vote
  if ((request_vote.last_log_term() < my_last_log_term) ||
      ((request_vote.last_log_term() == my_last_log_term) && (request_vote.last_log_index() < my_last_log_index))) {
    LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyRequestVote: Leader %s:%d last_log_term %lu is smaller than my %s:%d last_log_term term %lu,"
        " or Leader's last log term equal to my last_log_term, but Leader's last_log_index %lu is smaller than my last_log_index %lu",
        request_vote.ip().c_str(), request_vote.port(), request_vote.last_log_term(), options_.local_ip.c_str(), options_.local_port,
        my_last_log_term, request_vote.last_log_index(), my_last_log_index);
    BuildRequestVoteResponse(context_->current_term, granted, response);
    return -1;
  }

  if (vote_for_.find(request_vote.term()) != vote_for_.end()
      && vote_for_[request_vote.term()] != std::make_pair(request_vote.ip(), request_vote.port())) {
    LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyRequestVote: I %s:%d have voted for %s:%d in this term %lu",
        options_.local_ip.c_str(), options_.local_port, vote_for_[request_vote.term()].first.c_str(), 
        vote_for_[request_vote.term()].second, request_vote.term());
    BuildRequestVoteResponse(context_->current_term, granted, response);
    return -1;
  }
  vote_for_[request_vote.term()] = std::make_pair(request_vote.ip(), request_vote.port());
  LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyRequestVote: Receive Request Vote from %s:%d, "
      "Become Follower with current_term_(%lu) and new_term(%lu)"
      " commit_index(%lu) last_applied(%lu)", request_vote.ip().c_str(), request_vote.port(),
      context_->current_term, request_vote.last_log_term(), my_last_log_index, context_->last_applied.load());
  context_->BecomeFollower(request_vote.term());
  raft_meta_->SetCurrentTerm(context_->current_term);
  raft_meta_->SetVotedForIp(context_->voted_for_ip);
  raft_meta_->SetVotedForPort(context_->voted_for_port);
  // Got my vote
  GrantVote(request_vote.term(), request_vote.ip(), request_vote.port());
  granted = true;
  LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyRequestVote: Grant my vote to %s:%d at term %lu",
      context_->voted_for_ip.c_str(), context_->voted_for_port, context_->current_term);
  context_->last_op_time = slash::NowMicros();
  BuildRequestVoteResponse(context_->current_term, granted, response);
  return 0;
}

bool FloydImpl::AdvanceFollowerCommitIndex(uint64_t leader_commit) {
  // Update log commit index
  /*
   * If leaderCommit > commitIndex, set commitIndex =
   *   min(leaderCommit, index of last new entry)
   */
  context_->commit_index = std::min(leader_commit, raft_log_->GetLastLogIndex());
  raft_meta_->SetCommitIndex(context_->commit_index);
  return true;
}

int FloydImpl::ReplyAppendEntries(const CmdRequest& request, CmdResponse* response) {
  bool success = false;
  CmdRequest_AppendEntries append_entries = request.append_entries();
  slash::MutexLock l(&context_->global_mu);
  // update last_op_time to avoid another leader election
  context_->last_op_time = slash::NowMicros();
  // Ignore stale term
  // if the append entries leader's term is smaller than my current term, then the caller must an older leader
  if (append_entries.term() < context_->current_term) {
    LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries: Leader %s:%d term %lu is smaller than my %s:%d current term %lu",
        append_entries.ip().c_str(), append_entries.port(), append_entries.term(), options_.local_ip.c_str(), options_.local_port,
        context_->current_term);
    BuildAppendEntriesResponse(success, context_->current_term, raft_log_->GetLastLogIndex(), response);
    return -1;
  } else if ((append_entries.term() > context_->current_term) 
      || (append_entries.term() == context_->current_term && 
        (context_->role == kCandidate || (context_->role == kFollower && context_->leader_ip == "")))) {
    LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries: Leader %s:%d term %lu is larger than my %s:%d current term %lu, "
        "or leader term is equal to my current term, my role is %d, leader is [%s:%d]",
        append_entries.ip().c_str(), append_entries.port(), append_entries.term(), options_.local_ip.c_str(), options_.local_port,
        context_->current_term, context_->role, context_->leader_ip.c_str(), context_->leader_port);
    context_->BecomeFollower(append_entries.term(),
        append_entries.ip(), append_entries.port());
    raft_meta_->SetCurrentTerm(context_->current_term);
    raft_meta_->SetVotedForIp(context_->voted_for_ip);
    raft_meta_->SetVotedForPort(context_->voted_for_port);
  }

  if (append_entries.prev_log_index() > raft_log_->GetLastLogIndex()) {
    LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries: Leader %s:%d prev_log_index %lu is larger than my %s:%d last_log_index %lu",
        append_entries.ip().c_str(), append_entries.port(), append_entries.prev_log_index(), options_.local_ip.c_str(), options_.local_port,
        raft_log_->GetLastLogIndex());
    BuildAppendEntriesResponse(success, context_->current_term, raft_log_->GetLastLogIndex(), response);
    return -1;
  }

  // Append entry
  if (append_entries.prev_log_index() < raft_log_->GetLastLogIndex()) {
    LOGV(WARN_LEVEL, info_log_, "FloydImpl::ReplyAppendEtries: Leader %s:%d prev_log_index(%lu, %lu) is smaller than"
        " my last_log_index %lu, truncate suffix from %lu", append_entries.ip().c_str(), append_entries.port(),
        append_entries.prev_log_term(), append_entries.prev_log_index(), raft_log_->GetLastLogIndex(),
        append_entries.prev_log_index() + 1);
    raft_log_->TruncateSuffix(append_entries.prev_log_index() + 1);
  }

  // we compare peer's prev index and term with my last log index and term
  uint64_t my_last_log_term = 0;
  Entry entry;
  LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries "
      "prev_log_index: %lu\n", append_entries.prev_log_index());
  if (append_entries.prev_log_index() == 0) {
    my_last_log_term = 0;
  } else if (raft_log_->GetEntry(append_entries.prev_log_index(), &entry) == 0) {
    my_last_log_term = entry.term();
  } else {
    LOGV(WARN_LEVEL, info_log_, "FloydImple::ReplyAppentries: can't "
        "get Entry from raft_log prev_log_index %llu", append_entries.prev_log_index());
    BuildAppendEntriesResponse(success, context_->current_term, raft_log_->GetLastLogIndex(), response);
    return -1;
  }

  if (append_entries.prev_log_term() != my_last_log_term) {
    LOGV(WARN_LEVEL, info_log_, "FloydImpl::ReplyAppentries: leader %s:%d pre_log(%lu, %lu)'s term don't match with"
         " my log(%lu, %lu) term, truncate my log from %lu", append_entries.ip().c_str(), append_entries.port(),
         append_entries.prev_log_term(), append_entries.prev_log_index(), my_last_log_term, raft_log_->GetLastLogIndex(),
         append_entries.prev_log_index());
    // TruncateSuffix [prev_log_index, last_log_index)
    raft_log_->TruncateSuffix(append_entries.prev_log_index());
    BuildAppendEntriesResponse(success, context_->current_term, raft_log_->GetLastLogIndex(), response);
    return -1;
  }

  std::vector<const Entry*> entries;
  for (int i = 0; i < append_entries.entries().size(); i++) {
    entries.push_back(&append_entries.entries(i));
  }
  /*
   * for (auto& it : request.mutable_append_entries()->entries())) {
   *   entries.push_back(&it);
   * }
   */
  if (append_entries.entries().size() > 0) {
    LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries: Leader %s:%d will append %u entries from "
         " prev_log_index %lu", append_entries.ip().c_str(), append_entries.port(),
         append_entries.entries().size(), append_entries.prev_log_index());
    if (raft_log_->Append(entries) <= 0) {
      LOGV(ERROR_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries: Leader %s:%d ppend %u entries from "
          " prev_log_index %lu error at term %lu", append_entries.ip().c_str(), append_entries.port(),
          append_entries.entries().size(), append_entries.prev_log_index(), append_entries.term());
      BuildAppendEntriesResponse(success, context_->current_term, raft_log_->GetLastLogIndex(), response);
      return -1;
    }
  } else {
    LOGV(INFO_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries: Receive PingPong AppendEntries from %s:%d at term %lu",
        append_entries.ip().c_str(), append_entries.port(), append_entries.term());
  }
  if (append_entries.leader_commit() != context_->commit_index) {
    AdvanceFollowerCommitIndex(append_entries.leader_commit());
    apply_->ScheduleApply();
  }
  success = true;
  // only when follower successfully do appendentries, we will update commit index
  LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries server %s:%d Apply %d entries from Leader %s:%d"
      " prev_log_index %lu, leader commit %lu at term %lu", options_.local_ip.c_str(),
      options_.local_port, append_entries.entries().size(), append_entries.ip().c_str(),
      append_entries.port(), append_entries.prev_log_index(), append_entries.leader_commit(),
      append_entries.term());
  BuildAppendEntriesResponse(success, context_->current_term, raft_log_->GetLastLogIndex(), response);
  return 0;
}

}  // namespace floyd
