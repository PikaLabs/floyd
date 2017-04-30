#ifndef FLOYD_CLIENT_POOL_H_
#define FLOYD_CLIENT_POOL_H_

#include "floyd/src/command.pb.h"

#include "pink/include/pink_cli.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

using slash::Status;

class ClientPool;

class ClientPool {
 public:
  explicit ClientPool(int timeout_ms = 5000, int retry = 1);
  ~ClientPool();

  // Each try consists of Connect, Send and Recv;
  Status SendAndRecv(const std::string& server, const command::Command& req,
      command::CommandRes* res);

  Status UpHoldCli(pink::PinkCli *cli);

 private:
  int timeout_ms_;
  int retry_;
  slash::Mutex mu_;
  std::map<std::string, pink::PinkCli*> cli_map_;

  pink::PinkCli* GetClient(const std::string& server);

  ClientPool(const ClientPool&);
  bool operator=(const ClientPool&);
};

} // namespace floyd
#endif
