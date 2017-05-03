#ifndef FLOYD_PEER_THREAD_H_
#define FLOYD_PEER_THREAD_H_

#include "floyd/src/floyd_context.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "pink/include/bg_thread.h"

namespace floyd {

using slash::Status;

class Peer;

class FloydContext;
class FloydPrimary;
class FloydApply;

class FileLog;
class ClientPool;

struct FloydPeerEnv {
  std::string server;
  FloydContext* context;
  FloydPrimary* primary;
  FloydApply* apply;
  FileLog* log;
  ClientPool* pool;
  
  FloydPeerEnv(const std::string _server, FloydContext* _ctx, FloydPrimary* _pm,
               FloydApply* _apply, FileLog* _log, ClientPool* _pool)
    : server(_server),
      context(_ctx), 
      primary(_pm),
      apply(_apply),
      log(_log),
      pool(_pool) { }
};

class Peer {
 public:
  explicit Peer(FloydPeerEnv env);
  ~Peer();

  int StartThread();

  // Apend Entries
  void AddAppendEntriesTask();
  static void DoAppendEntries(void *arg);
  void AddHeartBeatTask();
  static void DoHeartBeat(void *arg);
  Status AppendEntries(bool heartbeat = false);

  // Request Vote
  void AddRequestVoteTask();
  Status RequestVote();
  static void DoRequestVote(void *arg);

  void BecomeLeader();

  uint64_t GetMatchIndex();
  void set_next_index(uint64_t next_index);
  uint64_t get_next_index();

 private:

  FloydPeerEnv env_;

  slash::Mutex mu_;
 // bool have_vote_;
  bool vote_done_;
  uint64_t next_index_;
  uint64_t match_index_;

  pink::BGThread bg_thread_;

  // No copying allowed
  Peer(const Peer&);
  void operator=(const Peer&);
};

} // namespace floyd
#endif
