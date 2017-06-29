#ifndef FLOYD_PEER_THREAD_H_
#define FLOYD_PEER_THREAD_H_

#include "floyd/src/floyd_context.h"

#include "slash/include/slash_status.h"
#include "pink/include/bg_thread.h"

namespace floyd {

using slash::Status;

class Peer;

class FloydContext;
class FloydPrimary;
//class FloydApply;
class RaftLog;
class ClientPool;

class Peer {
 public:
  Peer(std::string server, FloydContext* context, FloydPrimary* primary,
       RaftLog* raft_log, ClientPool* pool);
  ~Peer();

  int StartThread();

  // Apend Entries
  void AddAppendEntriesTask();
  void AddHeartBeatTask();
  void AddBecomeLeaderTask();
  static void DoAppendEntries(void *arg);
  Status AppendEntries();

  // Request Vote
  void AddRequestVoteTask();
  Status RequestVote();
  static void DoRequestVote(void *arg);

  uint64_t GetMatchIndex();
  void set_next_index(uint64_t next_index);
  uint64_t get_next_index();

 private:

  std::string server_;
  FloydContext* context_;
  FloydPrimary* primary_;
  RaftLog* raft_log_;
  ClientPool* pool_;

  std::atomic<uint64_t> next_index_;

  pink::BGThread bg_thread_;

  // No copying allowed
  Peer(const Peer&);
  void operator=(const Peer&);
};

} // namespace floyd
#endif
