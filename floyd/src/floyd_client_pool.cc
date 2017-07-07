// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/src/floyd_client_pool.h"

#include <unistd.h>
#include "floyd/src/logger.h"
#include "floyd/include/floyd_options.h"

#include "slash/include/slash_string.h"

namespace floyd {

static std::string CmdType(const CmdRequest& req);

ClientPool::ClientPool(Logger* info_log, int timeout_ms, int retry)
  : info_log_(info_log),
    timeout_ms_(timeout_ms),
    retry_(retry) {
}

Status ClientPool::SendAndRecv(const std::string& server, const CmdRequest& req, CmdResponse* res) {
  // LOGV(DEBUG_LEVEL, info_log_, "Client::SendAndRecv %s cmd to %s", CmdType(req).c_str(), server.c_str());
  Status ret;
  char stage = 0;
  Client *client = GetClient(server);
  pink::PinkCli* cli = client->cli;

  slash::MutexLock l(&client->mu);
  // TODO(anan) concurrent by epoll
  for (int i = 0; i < retry_; i++) {
    // Stage 0: Need Connect
    if ((stage & 0x01) == 0) {
      ret = UpHoldCli(client);
      if (!ret.ok()) {
        LOGV(WARN_LEVEL, info_log_, "Client::SendAndRecv %s cmd to %s, Connect Failed %s",
             CmdType(req).c_str(), server.c_str(), ret.ToString().c_str());
        cli->Close();
        usleep(5000);
        continue;
      }
      stage |= 0x01;
    }

    // Stage 1: Need Send
    if ((stage & 0x02) == 0) {
      ret = cli->Send((void*)&req);
      LOGV(DEBUG_LEVEL, info_log_, "Client::SendAndRecv %s cmd to %s Send return %s",
           CmdType(req).c_str(), server.c_str(), ret.ToString().c_str());
      if (ret.ok()) {
        stage |= 0x02;
      } else if (!ret.IsTimeout()) {
        cli->Close();
      }
    }

    // Stage 2: Need Recv
    if ((stage & 0x04) == 0) {
      ret = cli->Recv(res);
      LOGV(DEBUG_LEVEL, info_log_, "Client::SendAndRecv %s cmd to %s, Recv return %s",
           CmdType(req).c_str(), server.c_str(), ret.ToString().c_str());
      if (ret.ok()) {
        break;
      } else if (!ret.IsTimeout()) {
        cli->Close();
      }
    }
  }

  return ret;
}

ClientPool::~ClientPool() {
  slash::MutexLock l(&mu_);
  for (auto& iter : client_map_) {
    delete iter.second;
  }
  LOGV(DEBUG_LEVEL, info_log_, "ClientPool dtor");
}

Client* ClientPool::GetClient(const std::string& server) {
  slash::MutexLock l(&mu_);
  auto iter = client_map_.find(server);
  if (iter == client_map_.end()) {
    std::string ip;
    int port;
    slash::ParseIpPortString(server, ip, port);
    Client* client = new Client(ip, port);
    client_map_[server] = client;
    return client;
  } else {
    return iter->second;
  }
}

Status ClientPool::UpHoldCli(Client *client) {
  if (client == NULL || client->cli == NULL) {
    return Status::Corruption("null PinkCli");
  }

  Status ret;
  pink::PinkCli* cli = client->cli;
  if (!cli->Available()) {
    ret = cli->Connect();
    if (ret.ok()) {
      cli->set_send_timeout(timeout_ms_);
      cli->set_recv_timeout(timeout_ms_);
    }
  }
  return ret;
}

static std::string CmdType(const CmdRequest& cmd) {
  std::string ret;
  switch (cmd.type()) {
    case Type::Read: {
      ret = "Read";
      break;
    }
    case Type::ReadAll: {
      ret = "ReadAll";
      break;
    }
    case Type::DirtyWrite: {
      ret = "DirtyWrite";
      break;
    }
    case Type::Write: {
      ret = "Write";
      break;
    }
    case Type::Delete: {
      ret = "Delete";
      break;
    }
    case Type::TryLock: {
      ret = "TryLock";
      break;
    }
    case Type::UnLock: {
      ret = "UnLock";
      break;
    }
    case Type::DeleteUser: {
      ret = "DeleteUser";
      break;
    }
    case Type::RequestVote: {
      ret = "RequestVote";
      break;
    }
    case Type::AppendEntries: {
      ret = "AppendEntries";
      break;
    }
    case Type::ServerStatus: {
      ret = "ServerStatus";
      break;
    }
    default:
      ret = "UnknownCmd";
  }
  return ret;
}

} // namespace floyd
