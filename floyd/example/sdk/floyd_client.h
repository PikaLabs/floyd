#ifndef FLOYD_CLIENT_H
#define FLOYD_CLIENT_H

#include <string>
#include <vector>

#include "pink/include/pink_cli.h"
#include "slash/include/slash_status.h"


namespace floyd {
namespace client {

struct Option;
class Server;
class Cluster;

using slash::Status;

enum ClientError {
  kOk = 0,
};

class Server {
 public:
  std::string ip;
  int port;

  // colon separated ip:port
  Server(const std::string& str);
  Server(const std::string& _ip, const int& _port) : ip(_ip), port(_port) {}

  Server(const Server& server)
      : ip(server.ip),
      port(server.port) {}

  Server& operator=(const Server& server) {
    ip = server.ip;
    port = server.port;
    return *this;
  }

 private:
};

struct Option {
  // TODO session timeout
  int64_t timeout;
  Server server;

  Option(const std::string& _server);
  Option(const Option& option);
};

class Cluster {
 public:
  Cluster(const Option& option);

  Status Write(const std::string& key, const std::string& value);
  Status DirtyWrite(const std::string& key, const std::string& value);
  Status Read(const std::string& key, std::string* value);
  Status DirtyRead(const std::string& key, std::string* value);
  Status GetStatus(std::string *msg);
  Status Delete(const std::string& key);

 private:
  bool Init();

  Option option_;

  pink::PinkCli *pb_cli_;
};

} // namespace client
} // namespace floyd
#endif
