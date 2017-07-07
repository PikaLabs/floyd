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

// TODO(anan) typedef twice instead of include ?
class Peer;
typedef std::map<std::string, Peer*> PeersSet;

enum TaskType {
  kCheckElectLeader = 0,
  kLeaderHeartbeat,
  kBecomeLeader,
  kBecomeFollower,
  kNewCommand,
  kAdvanceCommitIndex
};

class FloydPrimary {
 public:
  FloydPrimary(FloydContext* context, FloydApply* apply);
  ~FloydPrimary();

  int Start();

  void AddTask(TaskType type, void *arg = NULL);

  void ResetElectLeaderTimer() {
    reset_elect_leader_time_ = slash::NowMicros();
  }
  void ResetLeaderHeartbeatTimer() {
    reset_leader_heartbeat_time_ = slash::NowMicros();
  }

  void set_peers(PeersSet* peers);

 private:

  FloydContext* context_;
  PeersSet* peers_;
  FloydApply* apply_;

  std::atomic<uint64_t> reset_elect_leader_time_;
  std::atomic<uint64_t> reset_leader_heartbeat_time_;
  pink::BGThread bg_thread_;

  // The Launch* work is done by floyd_peer_thread
  // Cron task
  static void LaunchLeaderHeartbeat(void *arg);
  static void LaunchCheckElectLeader(void *arg);

  // void LeaderHeartbeat();
  // void CheckElectLeader();

  static void LaunchBecomeLeader(void *arg);
  static void LaunchNewCommand(void *arg);
  static void LaunchAdvanceCommitIndex(void *arg);

  void NoticePeerTask(TaskType type);
  uint64_t QuorumMatchIndex();

  // No copying allowed
  FloydPrimary(const FloydPrimary&);
  void operator=(const FloydPrimary&);
};

} // namespace floyd
#endif  // FLOYD_SRC_FLOYD_PRIMARY_THREAD_H_
