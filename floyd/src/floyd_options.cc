// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "floyd/include/floyd_options.h"

namespace floyd {

void split(const std::string &str, char delim,
           std::vector<std::string> &tokens) {
  tokens.clear();
  size_t prev_pos = str.find_first_not_of(delim, 0);
  size_t pos = str.find(delim, prev_pos);

  while (prev_pos != std::string::npos || pos != std::string::npos) {
    std::string token(str.substr(prev_pos, pos - prev_pos));
    tokens.push_back(token);

    prev_pos = str.find_first_not_of(delim, pos);
    pos = str.find_first_of(delim, prev_pos);
  }
}

void Options::SetMembers(const std::string& cluster_string) {
  split(cluster_string, ',', members);
  if (members.size() == 1) {
    single_mode = true;
  }
}

void Options::Dump() {
  for (size_t i = 0; i < members.size(); i++) {
    printf ("               member %lu : %s\n", i, members[i].c_str());
  }
  printf ("                 local_ip : %s\n"
          "               local_port : %d\n"
          "                     path : %s\n"
          "         elect_timeout_ms : %ld\n"
          "             heartbeat_us : %ld\n"
          " append_entries_size_once : %ld\n"
          "append_entries_count_once : %lu\n"
          "              single_mode : %s\n",
            local_ip.c_str(),
            local_port,
            path.c_str(),
            elect_timeout_ms,
            heartbeat_us,
            append_entries_size_once,
            append_entries_count_once,
            single_mode ? "true" : "false");
}

std::string Options::ToString() {
  char str[1024];
  int len = 0;
  for (size_t i = 0; i < members.size(); i++) {
    len += sprintf (str + len, "                 member %lu : %s\n", i, members[i].c_str());
  }
  sprintf (str + len, "                 local_ip : %s\n"
          "               local_port : %d\n"
          "                     path : %s\n"
          "         elect_timeout_ms : %ld\n"
          "             heartbeat_us : %ld\n"
          " append_entries_size_once : %ld\n"
          "append_entries_count_once : %lu\n"
          "              single_mode : %s\n",
            local_ip.c_str(),
            local_port,
            path.c_str(),
            elect_timeout_ms,
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
    elect_timeout_ms(5000),
    heartbeat_us(1000000),
    append_entries_size_once(1024),
    append_entries_count_once(1),
    single_mode(false) {
    }

Options::Options(const std::string& cluster_string,
                 const std::string& _local_ip, int _local_port,
                 const std::string& _path)
  : local_ip(_local_ip),
    local_port(_local_port),
    path(_path),
    elect_timeout_ms(5000),
    heartbeat_us(1000000),
    append_entries_size_once(1024),
    append_entries_count_once(24),
    single_mode(false) {
  split(cluster_string, ',', members);
  if (members.size() == 1) {
    single_mode = true;
  }
}

} // namespace floyd
