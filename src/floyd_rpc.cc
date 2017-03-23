#include "floyd_rpc.h"

#include "status.h"
#include "command.pb.h"
#include "logger.h"

namespace floyd {

static std::string CmdType(command::Command& cmd) {
  std::string ret;
  switch (cmd.type()) {
    case command::Command::Read: {
      ret = "Read";
      break;
    }
    case command::Command::ReadAll: {
      ret = "ReadAll";
      break;
    }
    case command::Command::DirtyWrite: {
      ret = "DirtyWrite";
      break;
    }
    case command::Command::Write: {
      ret = "Write";
      break;
    }
    case command::Command::Delete: {
      ret = "Delete";
      break;
    }
    case command::Command::TryLock: {
      ret = "TryLock";
      break;
    }
    case command::Command::UnLock: {
      ret = "UnLock";
      break;
    }
    case command::Command::DeleteUser: {
      ret = "DeleteUser";
      break;
    }
    case command::Command::RaftVote: {
      ret = "RaftVote";
      break;
    }
    case command::Command::RaftAppendEntries: {
      ret = "RaftAppendEntries";
      break;
    }
    case command::Command::SynRaftStage: {
      ret = "SynRaftStage";
      break;
    }
    default:
      ret = "UnknownCmd";
  }
  return ret;
}

Status Rpc(NodeInfo* ni, command::Command& cmd, command::CommandRes& cmd_res) {
  LOG_DEBUG("MainThread: %s as Follower, redirect %s:%d", CmdType(cmd).c_str(),
            ni->ip.c_str(), ni->port);

  MutexLock l(&ni->dcc_mutex);
  Status ret = ni->UpHoldWorkerCliConn();
  if (!ret.ok()) return ret;

  ret = ni->dcc->SendMessage(&cmd);
  if (!ret.ok()) {
    LOG_WARN("MainThread::%s as Follower, redirect:SendMeassge fail: %s",
             CmdType(cmd).c_str(), ret.ToString().c_str());
    ni->dcc->Close();
    delete ni->dcc;
    ni->dcc = NULL;
    return ret;
  }
  LOG_DEBUG("MainThread::%s as Follower, redirect:SendMeassge success",
            CmdType(cmd).c_str());

  ret = ni->dcc->GetResMessage(&cmd_res);
  LOG_DEBUG("MainThread::%s as Follower, redirect:GetResMessage success",
            CmdType(cmd).c_str());
  if (!ret.ok()) {
    LOG_WARN("MainThread::%s as Follower, redirect:GetResMessage fail: %s",
             CmdType(cmd).c_str(), ret.ToString().c_str());
    ni->dcc->Close();
    delete ni->dcc;
    ni->dcc = NULL;
    return ret;
  }
  return ret;
}

command::Command BuildReadCommand(const std::string& key) {
  command::Command cmd;
  cmd.set_type(command::Command::Read);
  command::Command_Kv* kv = new command::Command_Kv();
  kv->set_key(key);
  cmd.set_allocated_kv(kv);
  return cmd;
}

command::Command BuildWriteCommand(const std::string& key,
                                   const std::string& value) {
  command::Command cmd;
  cmd.set_type(command::Command::Write);
  command::Command_Kv* kv = new command::Command_Kv();
  kv->set_key(key);
  kv->set_value(value);
  cmd.set_allocated_kv(kv);
  return cmd;
}

command::Command BuildDeleteCommand(const std::string& key) {
  command::Command cmd;
  cmd.set_type(command::Command::Delete);
  command::Command_Kv* kv = new command::Command_Kv();
  kv->set_key(key);
  cmd.set_allocated_kv(kv);
  return cmd;
}

command::Command BuildReadAllCommand() {
  command::Command cmd;
  cmd.set_type(command::Command::ReadAll);
  return cmd;
}
}
