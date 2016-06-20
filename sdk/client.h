#ifndef __FLOYD_CLIENT_H__
#define __FLOYD_CLIENT_H__

#include <string>
#include <vector>

#include "pb_cli.h"
#include "status.h"

namespace floyd {
namespace client {

struct Option;
class Server;
class Cluster;
class BadaPbCli;

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

  std::vector<Server> servers;

  Option();

  // comma separated server list:   ip1:port1,ip2:port2
  Option(const std::string& server_str);

  Option(const std::vector<std::string>& server_list); 
  Option(const Option& option);

  void ParseFromArgs(int argc, char *argv[]);
  void Init(const std::vector<std::string>& server_list);
};

class Cluster {
 public:
  Cluster(const Option& option);

  Status Put(const std::string& key, const std::string& value);

 private:
  void Init();

  Option option_;

  //pink::PbCli pb_cli_;

  // Bada use
  BadaPbCli *pb_cli_;
};


class BadaPbCli : public pink::PbCli {
 public:
  int32_t opcode_;

 private:
  virtual void BuildWbuf();

};

} // namespace client
} // namespace floyd
#endif
