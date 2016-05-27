#include "floyd_util.h"

namespace floyd {

Options::Options()
    : seed_ip("127.0.0.1"),
      seed_port(10086),
      local_ip("127.0.0.1"),
      local_port(10086),
      storage_type("leveldb"),
      data_path("/data/data"),
      log_type("SimpleFileLog"),
      log_path("/data/file"),
      elect_timeout_ms(5000),
      append_entries_size_once(10240) {}

//
// User Lock related
//
enum DataType {
  kLockType = 1,
  kUserMetaType,
  kUserDataType
};

std::string SerializeKey(const std::string key) {
  return kMetaPrefix + std::string(1, (char)kLockType) + key;
}

std::string ParseKey(const std::string encode_key) {
  assert(encode_key[0] == kMetaPrefix.at(0));
  assert(encode_key[1] == kLockType);
  return encode_key.substr(2);
}

std::string SerializeUser(const std::string &ip, const int port) {
  // return "#User#" + ip + ":" + std::to_string(port);
  return ip + ":" + std::to_string(port);
}

void ParseUser(const std::string &value, std::string *ip, int *port) {
  size_t pos = value.find(':');
  assert(pos != std::string::npos);

  *ip = value.substr(0, pos);
  *port = std::stol(value.substr(pos + 1));
}

std::string SerializeUserMeta(const std::string &user) {
  // return "#User#" + ip + ":" + std::to_string(port);
  return kMetaPrefix + std::string(1, (char)kUserMetaType) + user;
}

std::string SerializeUserData(const std::string &user, const std::string &key) {
  // return "#User#" + ip + ":" + std::to_string(port);
  return kMetaPrefix + std::string(1, (char)kUserDataType) + user + "=" + key;
}

void ParseUserData(const std::string &value, std::string *user,
                   std::string *key) {
  // return "#User#" + ip + ":" + std::to_string(port);
  size_t pos = value.find('=');
  assert(pos != std::string::npos);
  *user = value.substr(2, pos - 2);
  *key = value.substr(pos + 1);

  // return "#" + std::string(1, (char)kUserDataType) + user + "=" + key;
}
}
