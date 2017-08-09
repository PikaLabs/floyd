// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef FLOYD_INCLUDE_FLOYD_OPTIONS_H_
#define FLOYD_INCLUDE_FLOYD_OPTIONS_H_

#include <string>
#include <vector>

namespace floyd {

// Logger Level
enum {
  DEBUG_LEVEL = 0x01,
  INFO_LEVEL  = 0x02,
  WARN_LEVEL  = 0x03,
  ERROR_LEVEL = 0x04,
  FATAL_LEVEL = 0x05,
  NONE_LEVEL  = 0x06
};

struct Options {
  // cluster members
  // parsed from comma separated ip1:port1,ip2:port2...
  std::vector<std::string> members;
  std::string local_ip;
  int local_port;
  std::string path;
  uint64_t check_leader_us;
  uint64_t heartbeat_us;
  uint64_t append_entries_size_once;
  uint64_t append_entries_count_once;
  bool single_mode;

  void SetMembers(const std::string& cluster_string);

  void Dump();
  std::string ToString();

  Options();
  Options(const std::string& cluster_string,
          const std::string& _local_ip, int _local_port,
          const std::string& _path);
};

}  // namespace floyd
#endif  // FLOYD_INCLUDE_FLOYD_OPTIONS_H_
