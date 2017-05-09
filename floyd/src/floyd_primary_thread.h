#ifndef FLOYD_PRIMARY_THREAD_H_
#define FLOYD_PRIMARY_THREAD_H_

#include "floyd/src/floyd_context.h"

#include "slash/include/env.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "pink/include/bg_thread.h"

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

  void SetPeers(PeersSet* peers);

 private:

  FloydContext* context_;
  PeersSet* peers_;
  FloydApply* apply_;

  std::atomic<uint64_t> reset_leader_heartbeat_time_;
  std::atomic<uint64_t> reset_elect_leader_time_;
  pink::BGThread bg_thread_;

  // Cron task
  static void DoLeaderHeartbeat(void *arg);
  static void DoCheckElectLeader(void *arg);
  //void LeaderHeartbeat();
  //void CheckElectLeader();

  static void DoBecomeLeader(void *arg);
  static void DoNewCommand(void *arg);
  static void DoAdvanceCommitIndex(void *arg);

  void NoticePeerTask(TaskType type);
  uint64_t QuorumMatchIndex();

  // No copying allowed
  FloydPrimary(const FloydPrimary&);
  void operator=(const FloydPrimary&);
};

} // namespace floyd
#endif
