#include "floyd/src/command.pb.h"

#include <google/protobuf/text_format.h>
#include "floyd/include/floyd.h"
#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_rpc.h"
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/raft/file_log.h"
#include "floyd/src/logger.h"

#include "nemo-rocksdb/db_nemo_impl.h"
#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"

namespace floyd {

static command::Command BuildReadCommand(const std::string& key) {
  command::Command cmd;
  cmd.set_type(command::Command::Read);
  command::Command_Kv* kv = cmd.mutable_kv();
  kv->set_key(key);
  return cmd;
}

static command::CommandRes BuildReadResponse(const std::string &key,
    const std::string &value, bool succ) {
  command::CommandRes cmd_res;
  cmd_res.set_type(command::CommandRes::Read);
  command::CommandRes_KvRet* kvr = cmd_res.mutable_kvr();
  kvr->set_status(succ); if (succ) { kvr->set_value(value);
  }
  return cmd_res;
}

static command::Command BuildWriteCommand(const std::string& key,
    const std::string& value) {
  command::Command cmd;
  cmd.set_type(command::Command::Write);
  command::Command_Kv* kv = cmd.mutable_kv();
  kv->set_key(key);
  kv->set_value(value);
  return cmd;
}

static command::CommandRes BuildWriteResponse(bool succ) {
  command::CommandRes cmd_res;
  cmd_res.set_type(command::CommandRes::Write);
  command::CommandRes_KvRet* kvr = cmd_res.mutable_kvr();
  kvr->set_status(succ);
  return cmd_res;
}

static command::Command BuildDirtyWriteCommand(const std::string& key,
    const std::string& value) {
  command::Command cmd;
  cmd.set_type(command::Command::DirtyWrite);
  command::Command_Kv* kv = cmd.mutable_kv();
  kv->set_key(key);
  kv->set_value(value);
  return cmd;
}

static command::Command BuildDeleteCommand(const std::string& key) {
  command::Command cmd;
  cmd.set_type(command::Command::Delete);
  command::Command_Kv* kv = cmd.mutable_kv();
  kv->set_key(key);
  return cmd;
}

static command::CommandRes BuildDeleteResponse(bool succ) {
  command::CommandRes cmd_res;
  cmd_res.set_type(command::CommandRes::Delete);
  command::CommandRes_KvRet* kvr = cmd_res.mutable_kvr();
  kvr->set_status(succ);
  return cmd_res;
}

static command::CommandRes BuildRequestVoteResponse(uint64_t term,
    bool granted) {
  command::CommandRes cmd_res;
  cmd_res.set_type(command::CommandRes::RaftVote);
  floyd::raft::ResponseVote* rsv = cmd_res.mutable_rsv();
  rsv->set_term(term);
  rsv->set_granted(granted);
  return cmd_res;
}

static command::CommandRes BuildAppendEntriesResponse(uint64_t term,
    bool status) {
  command::CommandRes cmd_res;
  cmd_res.set_type(command::CommandRes::RaftAppendEntries);
  floyd::raft::AppendEntriesResponse* aers = cmd_res.mutable_aers();
  aers->set_term(term);
  aers->set_status(status);
  return cmd_res;
}

bool Floyd::HasLeader() {
  auto leader_node = context_->leader_node();
  return ((!leader_node.first.empty())
      && leader_node.second != 0);
}

static std::vector<Log::Entry*> BuildLogEntry(const command::Command& cmd,
    uint64_t current_term) {
  std::vector<Log::Entry*> entries;
  Log::Entry* entry = new Log::Entry;
  uint64_t len = cmd.ByteSize();
  char* data = new char[len + 1];
  cmd.SerializeToArray(data, len);
  entry->set_term(current_term);
  entry->set_cmd(data, len);
  delete data;
  entries.push_back(entry);
  return entries;
}

Status Floyd::Write(const std::string& key, const std::string& value) {
  if (!HasLeader()) {
    return Status::Incomplete("no leader node!");
  }
  command::Command cmd = BuildWriteCommand(key, value);
  command::CommandRes cmd_res;
  Status s = DoCommand(cmd, &cmd_res);
  if (!s.ok()) {
    return s;
  }
  if (cmd_res.kvr().status()) {
    return Status::OK();
  }
  return Status::Corruption("Write Error");
}

Status Floyd::DirtyWrite(const std::string& key, const std::string& value) {
  // Write myself first
  rocksdb::Status rs = db_->Put(rocksdb::WriteOptions(), key, value);
  if (!rs.ok()) {
    return Status::IOError("DirtyWrite failed, " + rs.ToString());
  }

  // Sync to other nodes without response
  command::Command cmd = BuildDirtyWriteCommand(key, value);

  command::CommandRes cmd_res;
  std::string local_server = slash::IpPortString(options_.local_ip, options_.local_port);
  for (auto& iter : options_.members) {
    if (iter != local_server) {
      Status s = worker_rpc_client_->SendRequest(iter, cmd, &cmd_res);
      LOG_DEBUG("Floyd::DirtyWrite Send to %s return %s, key(%s) value(%s)",
                iter.c_str(), s.ToString().c_str(), cmd.kv().key().c_str(), cmd.kv().value().c_str());
    }
  }
  return Status::OK();
}

Status Floyd::Delete(const std::string& key) {
  if (!HasLeader()) {
    return Status::Incomplete("no leader node!");
  }
  command::Command cmd = BuildDeleteCommand(key);
  command::CommandRes cmd_res;
  Status s = DoCommand(cmd, &cmd_res);
  if (!s.ok()) {
    return s;
  }
  if (cmd_res.kvr().status()) {
    return Status::OK();
  }
  return Status::Corruption("Delete Error");
}

Status Floyd::Read(const std::string& key, std::string& value) {
  if (!HasLeader()) {
    return Status::Incomplete("no leader node!");
  }
  command::Command cmd = BuildReadCommand(key);
  command::CommandRes cmd_res;
  Status s = DoCommand(cmd, &cmd_res);
  if (!s.ok()) {
    return s;
  }
  if (cmd_res.kvr().status()) {
    value = cmd_res.kvr().value();
    return Status::OK();
  }
  return Status::Corruption("Read Error");
}

Status Floyd::DirtyRead(const std::string& key, std::string& value) {
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
  if (s.ok()) {
    return Status::OK();
  }
  return Status::Corruption(s.ToString());
}

/* TODO wangkang-xy
// DirtyWrite
// ReadAll:
// TryLock:
// UnLock:
// DeleteUser:
// GetServerStatus
*/


Status Floyd::DoCommand(const command::Command& cmd,
    command::CommandRes *cmd_res) {
  // Execute if is leader
  std::pair<std::string, int> leader_node = context_->leader_node();
  if (options_.local_ip == leader_node.first
      && options_.local_port == leader_node.second) {
    return ExecuteCommand(cmd, cmd_res);
  }
  // Redirect to leader
  return worker_rpc_client_->SendRequest(
      slash::IpPortString(leader_node.first, leader_node.second),
      cmd, cmd_res);
}

Status Floyd::ExecuteDirtyCommand(const command::Command& cmd,
    command::CommandRes *cmd_res) {
  std::string value;
  rocksdb::Status rs;
  switch (cmd.type()) {
    case command::Command::DirtyWrite: {
      rs = db_->Put(rocksdb::WriteOptions(), cmd.kv().key(), cmd.kv().value());
      //TODO(anan) add response type or reorganize proto
      //cmd_res->set_type(command::CommandRes::DirtyWrite);
      cmd_res->set_type(command::CommandRes::Write);
      command::CommandRes_KvRet* kvr = cmd_res->mutable_kvr();
      if (rs.ok()) {
        kvr->set_status(true);
      } else {
        kvr->set_status(false);
      }

      LOG_DEBUG("Floyd::ExecuteDirtyCommand DirtyWrite %s, key(%s) value(%s)",
                rs.ToString().c_str(), cmd.kv().key().c_str(), cmd.kv().value().c_str());
#if (LOG_LEVEL != LEVEL_NONE)
      std::string text_format;
      google::protobuf::TextFormat::PrintToString(*cmd_res, &text_format);
      LOG_DEBUG("DirtyWrite Response :\n%s", text_format.c_str());
#endif
      break;
    }
    default: {
      return Status::Corruption("Unknown cmd type");
    }
  }
  return Status::OK();
}

Status Floyd::ExecuteCommand(const command::Command& cmd,
    command::CommandRes *cmd_res) {
  // Append entry local
  std::vector<Log::Entry*> entry = BuildLogEntry(cmd, context_->current_term());
  uint64_t last_index = (log_->Append(entry)).second;

  for (auto& iter : entry) {
    delete iter;
  }

  // Notify peers then wait for apply
  for (auto& peer : peers_) {
    peer.second->AddAppendEntriesTask();
  }
  Status res = context_->WaitApply(last_index, 1000);
  if (!res.ok()) {
    return res;
  } 
  
  // Complete command if needed
  std::string value;
  rocksdb::Status rs;
  switch (cmd.type()) {
    case command::Command::Write: {
      *cmd_res = BuildWriteResponse(true);
      //rs = db_->Put(rocksdb::WriteOptions(), cmd.kv().key(), cmd.kv().value());
      //if (rs.ok()) {
      //  *cmd_res = BuildWriteResponse(true);
      //} else {
      //  *cmd_res = BuildWriteResponse(false);
      //}
      //LOG_DEBUG("Floyd::ExecuteCommand Write %s, key(%s) value(%s)",
      //          rs.ToString().c_str(), cmd.kv().key().c_str(), cmd.kv().value().c_str());
      break;
    }
    case command::Command::Delete: {
      //rs = db_->Delete(rocksdb::WriteOptions(), cmd.kv().key());
      //if (rs.ok()) {
      //  *cmd_res = BuildDeleteResponse(true);
      //} else {
      //  *cmd_res = BuildDeleteResponse(false);
      //}
      //LOG_DEBUG("Floyd::ExecuteCommand Delete %s, key(%s)",
      //          rs.ToString().c_str(), cmd.kv().key().c_str());
      *cmd_res = BuildDeleteResponse(true);
      break;
    }
    case command::Command::Read: {
      rs = db_->Get(rocksdb::ReadOptions(), cmd.kv().key(), &value);
      //if (rs.ok()) {
      //  *cmd_res = BuildReadResponse(cmd.kv().key(),
      //                               value, true);
      //} else if (rs.IsNotFound()) {
      //  *cmd_res = BuildReadResponse(cmd.kv().key(),
      //                               value, false);
      //} else {
      //  *cmd_res = BuildReadResponse(cmd.kv().key(),
      //                               value, false);
      //}
      *cmd_res = BuildReadResponse(cmd.kv().key(),
          value, rs.ok());

      LOG_DEBUG("Floyd::ExecuteCommand Read %s, key(%s) value(%s)",
          rs.ToString().c_str(), cmd.kv().key().c_str(), value.c_str());
#if (LOG_LEVEL != LEVEL_NONE)
      std::string text_format;
      google::protobuf::TextFormat::PrintToString(*cmd_res, &text_format);
      LOG_DEBUG("ReadResponse :\n%s", text_format.c_str());
#endif
      break;
    }
    default: {
      return Status::Corruption("Unknown cmd type");
    }
  }
  return Status::OK();
  /* TODO wangkang-xy
  // case command::Command::ReadAll:
  // case command::Command::TryLock:
  // case command::Command::UnLock:
  // case command::Command::DeleteUser:
  */
}

void Floyd::DoRequestVote(command::Command& cmd,
    command::CommandRes* cmd_res) {
  // Step down by lager term
  bool granted = false;
  uint64_t my_term = context_->current_term();
  LOG_DEBUG("Floyd::DoRequestVote my_term=%lu rqv.term=%lu",
      my_term, cmd.rqv().term());
  if (cmd.rqv().term() < my_term) {
    BuildRequestVoteResponse(my_term, granted);
    return;
  }
  if (cmd.rqv().term() > my_term) {
    context_->BecomeFollower(cmd.rqv().term());
    leader_elect_timer_->Reset();
  }

  // Try to get my vote
  granted = context_->RequestVote(
      cmd.rqv().term(), cmd.rqv().ip(), cmd.rqv().port(),
      cmd.rqv().last_log_term(), cmd.rqv().last_log_index(),
      &my_term);

  *cmd_res = BuildRequestVoteResponse(my_term, granted);
}

void Floyd::DoAppendEntries(command::Command& cmd,
    command::CommandRes* cmd_res) {
  // Ignore stale term
  bool status = false;
  uint64_t my_term = context_->current_term();
  if (cmd.aerq().term() < my_term) {
    BuildAppendEntriesResponse(my_term, status);
    return;
  }
  context_->BecomeFollower(cmd.aerq().term(),
      cmd.aerq().ip(), cmd.aerq().port());
  leader_elect_timer_->Reset();
  
  std::vector<Log::Entry*> entries;
  for (auto& it : *(cmd.mutable_aerq()->mutable_entries())) {
    entries.push_back(&it);
  }
  // Append entries
  status = context_->AppendEntries(cmd.aerq().term(),
      cmd.aerq().prev_log_term(), cmd.aerq().prev_log_index(),
      entries, &my_term);

  // Update log commit index
  if (context_->AdvanceCommitIndex(cmd.aerq().commit_index())) {
    apply_->ScheduleApply();
  }

  *cmd_res = BuildAppendEntriesResponse(my_term, status);
}

}  // namespace floyd
