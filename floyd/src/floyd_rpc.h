#ifndef FLOYD_RPC_H_
#define FLOYD_RPC_H_

#include "floyd/src/command.pb.h"

#include "pink/include/pink_cli.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

using slash::Status;

class RpcClient;

class RpcClient {
 public:
  RpcClient();
  RpcClient(int timeout_ms);
  ~RpcClient();
  Status SendRequest(const std::string& server, const command::Command& req,
      command::CommandRes* res);

  Status UpHoldCli(pink::PinkCli *cli);

 private:
  //int retry;
  int timeout_ms_;
  slash::Mutex mu_;
  std::map<std::string, pink::PinkCli*> cli_map_;

  pink::PinkCli* GetClient(const std::string& server);

  RpcClient(const RpcClient&);
  bool operator=(const RpcClient&);
};

} // namespace floyd
#endif
