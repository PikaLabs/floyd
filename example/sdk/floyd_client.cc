#include "floyd_client.h"

#include <unistd.h>
#include <getopt.h>
#include <algorithm>
#include "logger.h"
#include "client.pb.h"

#include "pink_define.h"

namespace floyd {
namespace client {

void Tokenize(const std::string& str, std::vector<std::string>& tokens, const char& delimiter = ' ') {
  size_t prev_pos = str.find_first_not_of(delimiter, 0);
  size_t pos = str.find(delimiter, prev_pos);

  while (prev_pos != std::string::npos || pos != std::string::npos) {
    std::string token(str.substr(prev_pos, pos - prev_pos));
    //printf ("find a token(%s), prev_pos=%u pos=%u\n", token.c_str(), prev_pos, pos);
    tokens.push_back(token);

    prev_pos = str.find_first_not_of(delimiter, pos);
    pos = str.find_first_of(delimiter, prev_pos);
  }
}

///// Server //////
Server::Server(const std::string& str) {
  size_t pos = str.find(':');
  ip = str.substr(0, pos);
  port = atoi(str.substr(pos + 1).c_str());
}

///// Option //////
Option::Option()
    : timeout(1000) {
    }

Option::Option(const std::string& server_str) 
  : timeout(1000) {
  std::vector<std::string> server_list;
  Tokenize(server_str, server_list, ',');
  Init(server_list);
}

Option::Option(const std::vector<std::string>& server_list)
  : timeout(1000) {
  Init(server_list); 
}

Option::Option(const Option& option)
  : timeout(option.timeout) {
    std::copy(option.servers.begin(), option.servers.end(), std::back_inserter(servers));
  }


void Option::Init(const std::vector<std::string>& server_list) {
  for (auto it = server_list.begin(); it != server_list.end(); it++) {
    servers.push_back(Server(*it));
  }
}

void Option::ParseFromArgs(int argc, char *argv[]) {
  if (argc < 2) {
    printf ("Usage: ./client --server ip1:port1,ip2:port2\n");
    exit(-1);
  }

  static struct option const long_options[] = {
    {"server", required_argument, NULL, 's'},
    {NULL, 0, NULL, 0} };

  std::string server_str;
  int opt, optindex;
  while ((opt = getopt_long(argc, argv, "s:", long_options, &optindex)) != -1) {
    switch (opt) {
      case 's':
        server_str = optarg;
        break;
      default:
        break;
    }
  }

  std::vector<std::string> server_list;

  Tokenize(server_str, server_list, ',');
  Init(server_list);
}

////// Cluster //////
Cluster::Cluster(const Option& option)
  : option_(option),
  pb_cli_(new FloydPbCli) {
  Init();
}

bool Cluster::Init() {
  if (option_.servers.size() < 1) {
    LOG_ERROR("cluster has no server!");
    abort();
  }
  // TEST use the first server
  pink::Status result = pb_cli_->Connect(option_.servers[0].ip, option_.servers[0].port);
  if (!result.ok()) {
    LOG_ERROR("cluster connect error, %s", result.ToString().c_str());
    return false;
  }
  return true;
}

Status Cluster::Write(const std::string& key, const std::string& value) {

  Write_Request request;

  request.set_key(key);
  request.set_value(value);

  pb_cli_->set_opcode(OPCODE::WRITE);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }
  pink::Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Write_Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

  LOG_INFO("Write OK, status is %d, msg is %s\n", response.status(), response.msg().c_str());
  return Status::OK();
}

Status Cluster::Read(const std::string& key, std::string* value) {

  Read_Request request;
  request.set_key(key);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }

  pb_cli_->set_opcode(OPCODE::READ);

  pink::Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Read_Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

  *value = response.value();

  LOG_INFO("Read OK, status is %d, value is %s\n", response.status(), response.value().c_str());
  return Status::OK();
}

////// FloydPbCli //////
void FloydPbCli::BuildWbuf() {
  uint32_t len;
  wbuf_len_ = msg_->ByteSize();
  len = htonl(wbuf_len_ + 4);
  memcpy(wbuf_, &len, sizeof(uint32_t));
  len = htonl(opcode_);
  memcpy(wbuf_ + 4, &len, sizeof(uint32_t));
  msg_->SerializeToArray(wbuf_ + COMMAND_HEADER_LENGTH + 4, wbuf_len_);
  wbuf_len_ += 8;

  //printf ("wbuf_[0-4]  bytesize=%d len=%d\n", wbuf_len_, len);
}

} // namespace client
} // namspace floyd
