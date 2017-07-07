// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_impl.h"

#include <google/protobuf/text_format.h>

#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/floyd_primary_thread.h"
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/raft_log.h"
#include "floyd/src/logger.h"

#include "floyd/src/floyd.pb.h"

#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"

namespace floyd {

static void BuildReadRequest(const std::string& key, CmdRequest* cmd) {
  cmd->set_type(Type::Read);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
}

static void BuildReadResponse(const std::string &key, const std::string &value,
                              StatusCode code, CmdResponse* response) {
  //response->set_type(Type::Read);
  response->set_code(code);
  CmdResponse_Kv* kv = response->mutable_kv();
  if (code == StatusCode::kOk) {
    kv->set_value(value);
  }
}

static void BuildWriteRequest(const std::string& key,
                              const std::string& value, CmdRequest* cmd) {
  cmd->set_type(Type::Write);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
  kv->set_value(value);
}

static void BuildDirtyWriteRequest(const std::string& key,
                                   const std::string& value, CmdRequest* cmd) {
  cmd->set_type(Type::DirtyWrite);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
  kv->set_value(value);
}

static void BuildDeleteRequest(const std::string& key, CmdRequest* cmd) {
  cmd->set_type(Type::Delete);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
}

static void BuildRequestVoteResponse(uint64_t term, bool granted,
                                     CmdResponse* response) {
  response->set_type(Type::RequestVote);
  response->set_code(granted ? StatusCode::kOk : StatusCode::kError);
  CmdResponse_RequestVote* request_vote = response->mutable_request_vote();
  request_vote->set_term(term);
}

static void BuildAppendEntriesResponse(bool succ, uint64_t term,
                                       uint64_t log_index,
                                       CmdResponse* response) {
  response->set_type(Type::AppendEntries);
  response->set_code(succ ? StatusCode::kOk : StatusCode::kError);
  // response->set_code(StatusCode::kOk);
  CmdResponse_AppendEntries* append_entries = response->mutable_append_entries();
  append_entries->set_term(term);
  append_entries->set_last_log_index(log_index);
}

static void BuildLogEntry(const CmdRequest& cmd, uint64_t current_term,
                          Entry* entry) {
  uint64_t len = cmd.ByteSize();
  char* data = new char[len + 1];
  cmd.SerializeToArray(data, len);
  entry->set_term(current_term);
  entry->set_cmd(data, len);
  delete data;
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
  cmd.set_type(Type::ServerStatus);
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
    case Type::DirtyWrite: {
      rs = db_->Put(rocksdb::WriteOptions(), cmd.kv().key(), cmd.kv().value());
      //TODO(anan) add response type or reorganize proto
      //response->set_type(CmdResponse::DirtyWrite);
      response->set_type(Type::Write);
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
    case Type::ServerStatus: {
      response->set_type(Type::ServerStatus);
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
  case Type::Write: {
    response->set_code(StatusCode::kOk);
    break;
  }
  case Type::Delete: {
    response->set_code(StatusCode::kOk);
    break;
  }
  case Type::Read: {
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
  uint64_t my_term = context_->current_term();

  CmdRequest_RequestVote request_vote = cmd.request_vote();
  LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::DoRequestVote my_term=%lu rqv.term=%lu",
       my_term, request_vote.term());
  if (request_vote.term() < my_term) {
    BuildRequestVoteResponse(my_term, granted, response);
    return;
  }

  // Step down by larger term
  if (request_vote.term() > my_term) {
    context_->BecomeFollower(request_vote.term());
    primary_->ResetElectLeaderTimer();
  }

  // Try to get my vote
  granted = context_->ReceiverDoRequestVote(
      request_vote.term(), request_vote.ip(), request_vote.port(),
      request_vote.last_log_term(), request_vote.last_log_index(),
      &my_term);

  BuildRequestVoteResponse(my_term, granted, response);
}

void FloydImpl::ReplyAppendEntries(CmdRequest& cmd, CmdResponse* response) {
  // Ignore stale term
  bool status = false;
  uint64_t my_term = context_->current_term();
  CmdRequest_AppendEntries append_entries = cmd.append_entries();
  if (append_entries.term() < my_term) {
    BuildAppendEntriesResponse(status, my_term, raft_log_->GetLastLogIndex(), response);
    return;
  }
  context_->BecomeFollower(append_entries.term(),
                           append_entries.ip(), append_entries.port());
  std::vector<Entry*> entries;
  for (auto& it : *(cmd.mutable_append_entries()->mutable_entries())) {
    entries.push_back(&it);
  }

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

  // Update log commit index
  if (context_->AdvanceCommitIndex(append_entries.commit_index())) {
    LOGV(DEBUG_LEVEL, info_log_, "FloydImpl::ReplyAppendEntries after AdvanceCommitIndex %lu",
         context_->commit_index());
    apply_->ScheduleApply();
  }

  // TODO(anan) ElectLeader timer may timeout because of slow AppendEntries
  //   we delay reset timer.
  primary_->ResetElectLeaderTimer();
  BuildAppendEntriesResponse(status, my_term, raft_log_->GetLastLogIndex(), response);
}

}  // namespace floyd
