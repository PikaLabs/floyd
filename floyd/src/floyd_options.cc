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
}

void Options::Dump() {
  for (size_t i = 0; i < members.size(); i++) {
    printf ("               member %u : %s\n", i, members[i].c_str());
  }
  printf ("                 local_ip : %s\n"
          "               local_port : %d\n"
          "                data_path : %s\n"
          "                 log_path : %s\n"
          "         elect_timeout_ms : %ld\n"
          "             heartbeat_us : %ld\n"
          " append_entries_size_once : %ld\n",
            local_ip.c_str(),
            local_port,
            data_path.c_str(),
            log_path.c_str(),
            elect_timeout_ms,
            heartbeat_us,
            append_entries_size_once);
}

Options::Options()
  : local_ip("127.0.0.1"),
    local_port(10086),
    data_path("/data/data"),
    log_path("/data/file"),
    elect_timeout_ms(5000),
    heartbeat_us(200000),
    append_entries_size_once(10240) {
    }

Options::Options(const std::string& cluster_string,
                 const std::string& _local_ip, int _local_port,
                 const std::string& _data_path,
                 const std::string& _log_path)
  : local_ip(_local_ip),
    local_port(_local_port),
    data_path(_data_path),
    log_path(_log_path),
    elect_timeout_ms(5000),
    heartbeat_us(200000),
    append_entries_size_once(10240) {
  split(cluster_string, ',', members);
}

} // namespace floyd
