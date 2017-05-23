#ifndef FLOYD_CLIENT_POOL_H_
#define FLOYD_CLIENT_POOL_H_

#include "floyd/src/floyd.pb.h"

#include "pink/include/pink_cli.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

using slash::Status;

class Client;
class Logger;

class ClientPool {
 public:
  explicit ClientPool(Logger* info_log_, int timeout_ms = 1000, int retry = 1);
  ~ClientPool();

  // Each try consists of Connect, Send and Recv;
  Status SendAndRecv(const std::string& server, const CmdRequest& req,
      CmdResponse* res);

  Status UpHoldCli(Client* client);

 private:
  Logger* info_log_;
  int timeout_ms_;
  int retry_;
  slash::Mutex mu_;
  std::map<std::string, Client*> client_map_;

  Client* GetClient(const std::string& server);

  ClientPool(const ClientPool&);
  bool operator=(const ClientPool&);
};

struct Client {
  pink::PinkCli* cli;
  slash::Mutex mu;

  Client(const std::string& ip, int port) {
    cli = pink::NewPbCli(ip, port);
  }
};

} // namespace floyd
#endif
