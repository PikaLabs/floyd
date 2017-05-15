#ifndef FLOYD_OPTIONS_H_
#define FLOYD_OPTIONS_H_

#include <string>
#include <vector>

namespace floyd {

struct Options {
  // cluster members
  // parsed from comma separated ip1:port1,ip2:port2...
  std::vector<std::string> members; 

  std::string local_ip;
  int local_port;
  std::string data_path;
  std::string log_path;
  uint64_t elect_timeout_ms;
  uint64_t heartbeat_us;
  uint64_t append_entries_size_once;
  uint64_t append_entries_count_once;

  void SetMembers(const std::string& cluster_string);

  void Dump();
  std::string ToString();

  Options();
  Options(const std::string& cluster_string,
          const std::string& _local_ip, int _local_port,
          const std::string& _data_path,
          const std::string& _log_path);
};

} // namespace floyd
#endif
