#ifndef FLOYD_CLIENT_POOL_H_
#define FLOYD_CLIENT_POOL_H_

#include "floyd/src/floyd.pb.h"

#include "pink/include/pink_cli.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

using slash::Status;

class Logger;

class ClientPool {
 public:
  explicit ClientPool(Logger* info_log_, int timeout_ms = 5000, int retry = 1);
  ~ClientPool();

  // Each try consists of Connect, Send and Recv;
  Status SendAndRecv(const std::string& server, const CmdRequest& req,
      CmdResponse* res);

  Status UpHoldCli(pink::PinkCli *cli);

 private:
  Logger* info_log_;
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
