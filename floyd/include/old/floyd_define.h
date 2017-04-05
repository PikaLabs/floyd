#ifndef __FLOYED_DEFINE_H__
#define __FLOYED_DEFINE_H__

#include <map>

typedef std::map<std::string, std::string> KVMap;
enum NodeStatus {
  kUp = 0,
  kDown = 1
};

enum State {
  FOLLOWER = 0,
  CANDIDATE = 1,
  LEADER = 2,
};


#define FLOYD_PB_MAX_MESSAGE 10240
#define COMMAND_HEADER_LENGTH 4


#endif
