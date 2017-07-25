// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef FLOYD_SRC_FLOYD_PRIMARY_THREAD_H_
#define FLOYD_SRC_FLOYD_PRIMARY_THREAD_H_

#include <string>
#include <map>
#include <vector>


#include "slash/include/env.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "pink/include/bg_thread.h"

#include "floyd/src/floyd_context.h"

namespace floyd {

using slash::Status;

class FloydPrimary;

class FloydContext;
class FloydApply;
class Peer;
class Options;

typedef std::map<std::string, Peer*> PeersSet;

enum TaskType {
  kHeartBeat = 0,
  kCheckLeader = 1,
  kNewCommand = 2
};

class FloydPrimary {
 public:
  FloydPrimary(FloydContext* context, FloydApply* apply, const Options& options, Logger* info_log);
  ~FloydPrimary();

  int Start();

  void AddTask(TaskType type, bool is_delay = true, void *arg = NULL);

  void set_peers(PeersSet* peers);

 private:

  FloydContext* context_;
  FloydApply* apply_;
  PeersSet* peers_;
  Options options_;
  Logger* info_log_;

  std::atomic<uint64_t> reset_elect_leader_time_;
  std::atomic<uint64_t> reset_leader_heartbeat_time_;
  pink::BGThread bg_thread_;

  // The Launch* work is done by floyd_peer_thread
  // Cron task
  static void LaunchHeartBeatWrapper(void *arg);
  void LaunchHeartBeat();
  static void LaunchCheckLeaderWrapper(void *arg);
  void LaunchCheckLeader();
  static void LaunchNewCommandWrapper(void *arg);
  void LaunchNewCommand();

  void NoticePeerTask(TaskType type);
  uint64_t QuorumMatchIndex();

  // No copying allowed
  FloydPrimary(const FloydPrimary&);
  void operator=(const FloydPrimary&);
};

}  // namespace floyd
#endif  // FLOYD_SRC_FLOYD_PRIMARY_THREAD_H_
