#ifndef FLOYD_PRIMARY_THREAD_H_
#define FLOYD_PRIMARY_THREAD_H_

#include "floyd/src/floyd_context.h"

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

namespace raft {
class Log;
}

struct FloydPrimaryEnv {
  FloydContext* context;
  PeersSet* peers;
  FloydApply* apply;
  Log* log;
  
  FloydPrimaryEnv(FloydContext* _ctx, PeersSet* _peers,
               FloydApply* _apply, Log* _log)
    : context(_ctx), 
      peers(_peers),
      apply(_apply),
      log(_log) { }

  FloydPrimaryEnv(FloydContext* _ctx, FloydApply* _apply, Log* _log)
    : context(_ctx), 
      apply(_apply),
      log(_log) { }
};

enum TaskType {
  kCheckElectLeader = 0,
  kBecomeLeader,
  // TODO(anan) heartbeat triggered by MainThread
  //kHeartbeat,
  kBecomeFollower
};

class FloydPrimary {
 public:
  explicit FloydPrimary(FloydPrimaryEnv env);
  ~FloydPrimary();

  int Start();

  void AddTask(TaskType type, void *arg = NULL);

  static void DoCheckElectLeader(void *arg);
  void CheckElectLeader();

  static void DoBecomeLeader(void *arg);
  void BecomeLeader();

  void ResetElectLeaderTimer() {
    elect_leader_reset_ = true;
  }

  void SetPeers(PeersSet* peers);

  uint64_t QuorumMatchIndex();
  void AdvanceCommitIndex();

 private:

  FloydPrimaryEnv env_;
  std::atomic<bool> elect_leader_reset_;
  pink::BGThread bg_thread_;

  // No copying allowed
  FloydPrimary(const FloydPrimary&);
  void operator=(const FloydPrimary&);
};

} // namespace floyd
#endif
