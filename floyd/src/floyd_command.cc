#include "floyd/src/floyd_impl.h"

#include <google/protobuf/text_format.h>

#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/floyd_primary_thread.h"
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/file_log.h"
#include "floyd/src/logger.h"

#include "floyd/src/floyd.pb.h"

#include "nemo-rocksdb/db_nemo_impl.h"
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
  response->set_type(Type::Read);
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

static void BuildWriteResponse(StatusCode code, CmdResponse* response) {
  response->set_type(Type::Write);
  response->set_code(code);
}

static void BuildDirtyWriteRequest(const std::string& key,
    const std::string& value, CmdRequest* cmd) {
  cmd->set_type(Type::DirtyWrite);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
  kv->set_value(value);
}

static void BuildDeleteRequest(const std::string& key,
                               CmdRequest* cmd) {
  cmd->set_type(Type::Delete);
  CmdRequest_Kv* kv = cmd->mutable_kv();
  kv->set_key(key);
}

static void BuildDeleteResponse(StatusCode code, CmdResponse* response) {
  response->set_type(Type::Delete);
  response->set_code(code);
}

static void BuildRequestVoteResponse(uint64_t term, bool granted,
                                     CmdResponse* response) {
  response->set_type(Type::RequestVote);
  response->set_code(granted ? StatusCode::kOk : StatusCode::kError);
  CmdResponse_RequestVote* request_vote = response->mutable_request_vote();
  request_vote->set_term(term);
}

static void BuildAppendEntriesResponse(uint64_t term, bool succ,
                                       CmdResponse* response) {
  response->set_type(Type::AppendEntries);
  response->set_code(succ ? StatusCode::kOk : StatusCode::kError);
  CmdResponse_AppendEntries* append_entries = response->mutable_append_entries();
  append_entries->set_term(term);
}

bool FloydImpl::HasLeader() {
  auto leader_node = context_->leader_node();
  return ((!leader_node.first.empty())
      && leader_node.second != 0);
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
      LOG_DEBUG("FloydImpl::DirtyWrite Send to %s return %s, key(%s) value(%s)",
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

//#if (LOG_LEVEL != LEVEL_NONE)
//  std::string text_format;
//  google::protobuf::TextFormat::PrintToString(cmd, &text_format);
//  LOG_DEBUG("Delete CmdRequest :\n%s", text_format.c_str());
//#endif
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
  LOG_DEBUG("FloydImpl::GetServerStatus start");

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
            server_status.last_apply_index());

  msg.clear();
  msg.append(str);

  CmdRequest cmd;
  cmd.set_type(Type::ServerStatus);
  CmdResponse response;
  std::string local_server = slash::IpPortString(options_.local_ip, options_.local_port);
  for (auto& iter : options_.members) {
    if (iter != local_server) {
      Status s = worker_client_pool_->SendAndRecv(iter, cmd, &response);
      LOG_DEBUG("FloydImpl::GetServerStatus Send to %s return %s",
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
                  server_status.last_apply_index());
        msg.append(str);
        LOG_DEBUG("GetServerStatus msg(%s)", str);
      }
    }
  }
  return true;
}

Status FloydImpl::DoCommand(const CmdRequest& cmd,
    CmdResponse *response) {
  // Execute if is leader
  std::pair<std::string, int> leader_node = context_->leader_node();
  if (options_.local_ip == leader_node.first
      && options_.local_port == leader_node.second) {
    return ExecuteCommand(cmd, response);
  }
  // Redirect to leader
  return worker_client_pool_->SendAndRecv(
      slash::IpPortString(leader_node.first, leader_node.second),
      cmd, response);
}

Status FloydImpl::ExecuteDirtyCommand(const CmdRequest& cmd,
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

      LOG_DEBUG("FloydImpl::ExecuteDirtyCommand DirtyWrite %s, key(%s) value(%s)",
                rs.ToString().c_str(), cmd.kv().key().c_str(), cmd.kv().value().c_str());
#if (LOG_LEVEL != LEVEL_NONE)
      std::string text_format;
      google::protobuf::TextFormat::PrintToString(*response, &text_format);
      LOG_DEBUG("DirtyWrite Response :\n%s", text_format.c_str());
#endif
      break;
    }
    case Type::ServerStatus: {
      response->set_type(Type::ServerStatus);
      CmdResponse_ServerStatus* server_status = response->mutable_server_status();
      DoGetServerStatus(server_status);
      LOG_DEBUG("FloydImpl::ExecuteDirtyCommand GetServerStatus");
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

  auto leader_node = context_->leader_node();
  if (leader_node.first.empty()) {
    res->set_leader_ip("null");
  } else {
    res->set_leader_ip(leader_node.first);
  }
  res->set_leader_port(leader_node.second);

  auto voted_for_node = context_->voted_for_node();
  if (voted_for_node.first.empty()) {
    res->set_voted_for_ip("null");
  } else {
    res->set_voted_for_ip(voted_for_node.first);
  }
  res->set_voted_for_port(voted_for_node.second);
  
  uint64_t last_log_index;
  uint64_t last_log_term;
  log_->GetLastLogTermAndIndex(&last_log_term, &last_log_index);

  res->set_last_log_term(last_log_term);
  res->set_last_log_index(last_log_index);
  res->set_last_apply_index(context_->apply_index());
  return true;
}

Status FloydImpl::ExecuteCommand(const CmdRequest& cmd,
    CmdResponse *response) {
  // Append entry local
  std::vector<Entry*> entries;
  Entry entry;
  BuildLogEntry(cmd, context_->current_term(), &entry);
  entries.push_back(&entry);
  uint64_t last_index = (log_->Append(entries)).second;

  // Notify primary then wait for apply
  primary_->AddTask(kNewCommand);

  Status res = context_->WaitApply(last_index, 1000);
  if (!res.ok()) {
    return res;
  } 
  
  // Complete CmdRequest if needed
  std::string value;
  rocksdb::Status rs;
  switch (cmd.type()) {
    case Type::Write: {
      BuildWriteResponse(StatusCode::kOk, response);
      break;
    }
    case Type::Delete: {
      BuildDeleteResponse(StatusCode::kOk, response);
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
      LOG_DEBUG("FloydImpl::ExecuteCommand Read %s, key(%s) value(%s)",
          rs.ToString().c_str(), cmd.kv().key().c_str(), value.c_str());
#if (LOG_LEVEL != LEVEL_NONE)
      std::string text_format;
      google::protobuf::TextFormat::PrintToString(*response, &text_format);
      LOG_DEBUG("ReadResponse :\n%s", text_format.c_str());
#endif
      break;
    }
    default: {
      return Status::Corruption("Unknown cmd type");
    }
  }
  return Status::OK();
}

void FloydImpl::DoRequestVote(CmdRequest& cmd,
    CmdResponse* response) {
  // Step down by lager term
  bool granted = false;
  uint64_t my_term = context_->current_term();

  CmdRequest_RequestVote request_vote = cmd.request_vote();
  LOG_DEBUG("FloydImpl::DoRequestVote my_term=%lu rqv.term=%lu",
      my_term, request_vote.term());
  if (request_vote.term() < my_term) {
    BuildRequestVoteResponse(my_term, granted, response);
    return;
  }
  if (request_vote.term() > my_term) {
    context_->BecomeFollower(request_vote.term());
    primary_->ResetElectLeaderTimer();
  }

  // Try to get my vote
  granted = context_->RequestVote(
      request_vote.term(), request_vote.ip(), request_vote.port(),
      request_vote.last_log_term(), request_vote.last_log_index(),
      &my_term);

  BuildRequestVoteResponse(my_term, granted, response);
}

void FloydImpl::DoAppendEntries(CmdRequest& cmd,
    CmdResponse* response) {
  // Ignore stale term
  bool status = false;
  uint64_t my_term = context_->current_term();
  CmdRequest_AppendEntries append_entries = cmd.append_entries();
  if (append_entries.term() < my_term) {
    BuildAppendEntriesResponse(my_term, status, response);
    return;
  }
  context_->BecomeFollower(append_entries.term(),
      append_entries.ip(), append_entries.port());
  primary_->ResetElectLeaderTimer();
  
  std::vector<Entry*> entries;
  for (auto& it : *(cmd.mutable_append_entries()->mutable_entries())) {
    entries.push_back(&it);
  }
  // Append entries
  status = context_->AppendEntries(append_entries.term(),
      append_entries.prev_log_term(), append_entries.prev_log_index(),
      entries, &my_term);

  // Update log commit index
  if (context_->AdvanceCommitIndex(append_entries.commit_index())) {
    apply_->ScheduleApply();
  }

  BuildAppendEntriesResponse(my_term, status, response);
}

}  // namespace floyd
