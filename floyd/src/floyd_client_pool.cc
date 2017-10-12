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

static std::string CmdType(const CmdRequest& cmd) {
  std::string ret;
  switch (cmd.type()) {
    case Type::kRead:
      ret = "Read";
      break;
    case Type::kWrite:
      ret = "Write";
      break;
    case Type::kDelete:
      ret = "Delete";
      break;
    case Type::kTryLock:
      ret = "TryLock";
      break;
    case Type::kUnLock:
      ret = "UnLock";
      break;
    case Type::kRequestVote:
      ret = "RequestVote";
      break;
    case Type::kAppendEntries:
      ret = "AppendEntries";
      break;
    case Type::kServerStatus:
      ret = "ServerStatus";
      break;
    default:
      ret = "UnknownCmd";
  }
  return ret;
}


ClientPool::ClientPool(Logger* info_log, int timeout_ms, int retry)
  : info_log_(info_log),
    timeout_ms_(timeout_ms),
    retry_(retry) {
}

// sleep 1 second after each send message
Status ClientPool::SendAndRecv(const std::string& server, const CmdRequest& req, CmdResponse* res) {
  /*
   * if (req.type() == kAppendEntries) {
   *   LOGV(INFO_LEVEL, info_log_, "ClientPool::SendAndRecv to %s"
   *       " Request type %s prev_log_index %lu prev_log_term %lu leader_commit %lu "
   *       "append entries size %d at term %d", server.c_str(),
   *       CmdType(req).c_str(), req.append_entries().prev_log_index(), req.append_entries().prev_log_term(),
   *       req.append_entries().leader_commit(), req.append_entries().entries().size(), req.append_entries().term());
   * }
   */
  LOGV(DEBUG_LEVEL, info_log_, "ClientPool::SendAndRecv Send %s command to server %s", CmdType(req).c_str(), server.c_str());
  Client *client = GetClient(server);
  pink::PinkCli* cli = client->cli;

  Status ret = Status::Incomplete("Not send");
  slash::MutexLock l(&client->mu);
  ret = UpHoldCli(client);
  if (!ret.ok()) {
    if (req.type() == kAppendEntries) {
      LOGV(WARN_LEVEL, info_log_, "ClientPool::SendAndRecv Server Connect to %s failed, error reason: %s"
          " Request type %s prev_log_index %lu prev_log_term %lu leader_commit %lu "
          "append entries size %d at term %d", server.c_str(), ret.ToString().c_str(),
          CmdType(req).c_str(), req.append_entries().prev_log_index(), req.append_entries().prev_log_term(),
          req.append_entries().leader_commit(), req.append_entries().entries().size(), req.append_entries().term());
    } else {
      LOGV(WARN_LEVEL, info_log_, "ClientPool::SendAndRecv Server Connect to %s failed, error reason: %s"
          " Request type %s", server.c_str(), ret.ToString().c_str(), CmdType(req).c_str());
      sleep(1);
    }
    cli->Close();
    return ret;
  }

  ret = cli->Send((void *)(&req));
  if (!ret.ok()) {
    if (req.type() == kAppendEntries) {
      LOGV(WARN_LEVEL, info_log_, "ClientPool::SendAndRecv Server Send to %s failed, error reason: %s"
          " Request type %s prev_log_index %lu prev_log_term %lu leader_commit %lu "
          "append entries size %d at term %d", server.c_str(), ret.ToString().c_str(),
          CmdType(req).c_str(), req.append_entries().prev_log_index(), req.append_entries().prev_log_term(),
          req.append_entries().leader_commit(), req.append_entries().entries().size(), req.append_entries().term());
    } else {
      LOGV(WARN_LEVEL, info_log_, "ClientPool::SendAndRecv Server Send to %s failed, error reason: %s"
          " Request type %s", server.c_str(), ret.ToString().c_str(), CmdType(req).c_str());
      sleep(1);
    }
    cli->Close();
    return ret;
  }

  ret = cli->Recv(res);
  if (!ret.ok()) {
    if (req.type() == kAppendEntries) {
      LOGV(WARN_LEVEL, info_log_, "ClientPool::SendAndRecv Server Recv to %s failed, error reason: %s"
          " Request type %s prev_log_index %lu prev_log_term %lu leader_commit %lu "
          "append entries size %d at term %d", server.c_str(), ret.ToString().c_str(),
          CmdType(req).c_str(), req.append_entries().prev_log_index(), req.append_entries().prev_log_term(),
          req.append_entries().leader_commit(), req.append_entries().entries().size(), req.append_entries().term());
    } else {
      LOGV(WARN_LEVEL, info_log_, "ClientPool::SendAndRecv Server Recv to %s failed, error reason: %s"
          " Request type %s", server.c_str(), ret.ToString().c_str(), CmdType(req).c_str());
      sleep(1);
    }
    cli->Close();
    return ret;
  }
  if (ret.ok()) {
    if (res->code() == StatusCode::kOk || res->code() == StatusCode::kNotFound) {
      return Status::OK();
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

} // namespace floyd
