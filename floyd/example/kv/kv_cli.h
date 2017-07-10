#ifndef FLOYD_CLIENT_H
#define FLOYD_CLIENT_H

#include <string>
#include <vector>

#include "pink/include/pink_cli.h"
#include "slash/include/slash_status.h"


namespace floyd {

struct Option;
class Server;
class Cluster;

using slash::Status;

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

// Logger Level
enum LogLevel {
  DEBUG_LEVEL = 0x01,
  INFO_LEVEL  = 0x02,
  WARN_LEVEL  = 0x03,
  ERROR_LEVEL = 0x04,
  FATAL_LEVEL = 0x05,
  NONE_LEVEL  = 0x06
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
  Status set_log_level(const int log_level);

 private:
  bool Init();

  Option option_;

  pink::PinkCli *pb_cli_;
};

} // namespace floyd
#endif
