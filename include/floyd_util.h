#ifndef __FLOYD_UTIL_H__
#define __FLOYD_UTIL_H__

#include <string>
#include <vector>
#include <sys/time.h>
#include <time.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <memory>
#include <deque>
#include "csapp.h"

namespace floyd {
struct Options {
  // seed
  std::string seed_ip;
  int seed_port;

  // local
  std::string local_ip;
  int local_port;
  std::string storage_type;
  std::string data_path;

  // raft
  std::string log_type;
  std::string log_path;
  uint64_t elect_timeout_ms;
  uint64_t append_entries_size_once;

  Options();
};

//
// User Lock related
//
// enum DataType {
//  kLockType = 1,
//  kUserMetaType,
//  kUserDataType
//};

const std::string kMetaPrefix = "#";

std::string SerializeKey(const std::string key);
std::string ParseKey(const std::string encode_key);

std::string SerializeUser(const std::string &ip, const int port);
void ParseUser(const std::string &value, std::string *ip, int *port);

std::string SerializeUserMeta(const std::string &user);
std::string SerializeUserData(const std::string &user, const std::string &key);
void ParseUserData(const std::string &value, std::string *user,
                   std::string *key);
}

#endif
