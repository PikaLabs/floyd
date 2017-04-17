#include "floyd/src/floyd_rpc.h"

#include "floyd/src/logger.h"

#include "slash/include/slash_string.h"

namespace floyd {

static std::string CmdType(const command::Command& req);

RpcClient::RpcClient()
  : timeout_ms_(2000) {
}

RpcClient::RpcClient(int timeout_ms)
  : timeout_ms_(timeout_ms) {
}

Status RpcClient::SendRequest(const std::string& server, const command::Command& req, command::CommandRes* res) {
  LOG_DEBUG("Client::SendRequest %s cmd to %s", CmdType(req).c_str(), server.c_str());

  pink::PinkCli *cli = GetClient(server);

  Status ret = UpHoldCli(cli);
  if (!ret.ok()) return ret;

  ret = cli->Send((void*)&req);
  LOG_DEBUG("Client::SendRequest %s cmd to %s Send return %s", CmdType(req).c_str(), server.c_str(),
              ret.ToString().c_str());
  if (ret.ok()) {
    ret = cli->Recv(res);
    LOG_WARN("Client::SendRequest %s cmd to %s, Recv return %s", CmdType(req).c_str(), server.c_str(),
              ret.ToString().c_str());
  }

  return ret;
}

RpcClient::~RpcClient() {
  slash::MutexLock l(&mu_);
  for (auto& iter : cli_map_) {
    delete iter.second;
  }
}

pink::PinkCli* RpcClient::GetClient(const std::string& server) {
  slash::MutexLock l(&mu_);
  auto iter = cli_map_.find(server);
  if (iter == cli_map_.end()) {
    std::string ip;
    int port;
    slash::ParseIpPortString(server, ip, port);
    pink::PinkCli* cli = pink::NewPbCli(ip, port);
    cli_map_[server] = cli;
    return cli;
  } else {
    return iter->second;
  }
}

Status RpcClient::UpHoldCli(pink::PinkCli *cli) {
  Status ret;
  if (cli == NULL || !cli->Available()) {
    if (cli != NULL) {
      cli->Close();
    }
    ret = cli->Connect();
    if (ret.ok()) {
      cli->set_send_timeout(timeout_ms_);
      cli->set_recv_timeout(timeout_ms_);
    }
  }
  return ret;
}

static std::string CmdType(const command::Command& cmd) {
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

} // namespace floyd
