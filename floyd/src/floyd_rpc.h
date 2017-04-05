#ifndef FLOYD_RPC_H_
#define FLOYD_RPC_H_

#include "floyd/include/command.pb.h"

#include "pink/include/pink_cli.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

class RpcClient;

class RpcClient {
 public:
  RpcClient();
  bool SendRequest(const std::string& server, command::Command& req, command::Command& res);

  Status RpcClient::UpHoldCli(slash::PinkCli *cli);

 private:
  slash::Mutex mu_;
  std::map<std::string, slash::PinkCli*> cli_map_;

  pink::PinkCli* RpcClient::GetClient(const std::string& server);

  RpcClient(const RpcClient&);
  bool operator=(const RpcClient&);
};

//Status Rpc(NodeInfo* ni, command::Command& cmd, command::CommandRes& cmd_res);
//command::Command BuildWriteCommand(const std::string& key,
//                                   const std::string& value);
//command::Command BuildReadAllCommand();
//command::Command BuildReadCommand(const std::string& key);
//command::Command BuildDeleteCommand(const std::string& key);

}
#endif
