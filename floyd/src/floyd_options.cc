// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/include/floyd_options.h"

#include <cstdlib>
#include <ctime>

#include "slash/include/env.h"

namespace floyd {

static void split(const std::string &str, char delim,
           std::vector<std::string> *tokens) {
  tokens->clear();
  size_t prev_pos = str.find_first_not_of(delim, 0);
  size_t pos = str.find(delim, prev_pos);

  while (prev_pos != std::string::npos || pos != std::string::npos) {
    std::string token(str.substr(prev_pos, pos - prev_pos));
    tokens->push_back(token);

    prev_pos = str.find_first_not_of(delim, pos);
    pos = str.find_first_of(delim, prev_pos);
  }
}

void Options::SetMembers(const std::string& cluster_string) {
  split(cluster_string, ',', &members);
  if (members.size() == 1) {
    single_mode = true;
  }
}

void Options::Dump() {
  for (size_t i = 0; i < members.size(); i++) {
    printf("               member %lu : %s\n", i, members[i].c_str());
  }
  printf("                 local_ip : %s\n"
          "               local_port : %d\n"
          "                     path : %s\n"
          "          check_leader_us : %ld\n"
          "             heartbeat_us : %ld\n"
          " append_entries_size_once : %ld\n"
          "append_entries_count_once : %lu\n"
          "              single_mode : %s\n",
            local_ip.c_str(),
            local_port,
            path.c_str(),
            check_leader_us,
            heartbeat_us,
            append_entries_size_once,
            append_entries_count_once,
            single_mode ? "true" : "false");
}

std::string Options::ToString() {
  char str[1024];
  int len = 0;
  for (size_t i = 0; i < members.size(); i++) {
    len += snprintf(str + len, sizeof(str), "                 member %lu : %s\n", i, members[i].c_str());
  }
  snprintf(str + len, sizeof(str), "                 local_ip : %s\n"
          "               local_port : %d\n"
          "                     path : %s\n"
          "          check_leader_us : %ld\n"
          "             heartbeat_us : %ld\n"
          " append_entries_size_once : %ld\n"
          "append_entries_count_once : %lu\n"
          "              single_mode : %s\n",
            local_ip.c_str(),
            local_port,
            path.c_str(),
            check_leader_us,
            heartbeat_us,
            append_entries_size_once,
            append_entries_count_once,
            single_mode ? "true" : "false");
  return str;
}

Options::Options()
  : local_ip("127.0.0.1"),
    local_port(10086),
    path("/data/floyd"),
    check_leader_us(6000000),
    heartbeat_us(3000000),
    append_entries_size_once(10240000),
    append_entries_count_once(102400),
    single_mode(false) {
    }

Options::Options(const std::string& cluster_string,
                 const std::string& _local_ip, int _local_port,
                 const std::string& _path)
  : local_ip(_local_ip),
    local_port(_local_port),
    path(_path),
    check_leader_us(6000000),
    heartbeat_us(3000000),
    append_entries_size_once(10240000),
    append_entries_count_once(102400),
    single_mode(false) {
  std::srand(slash::NowMicros());
  // the default check_leader is [3s, 5s)
  // the default heartbeat time is 1s
  // we can promise 1s + 2 * rpc < 3s, since rpc time is approximately 10ms
  check_leader_us = std::rand() % 2000000 + check_leader_us;
  split(cluster_string, ',', &members);
  if (members.size() == 1) {
    single_mode = true;
  }
}

} // namespace floyd
