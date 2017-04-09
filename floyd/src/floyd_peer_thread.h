#ifndef FLOYD_PEER_THREAD_H_
#define FLOYD_PEER_THREAD_H_

#include "floyd/include/floyd.h"
#include "floyd/src/floyd_context.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "pink/include/bg_thread.h"

namespace floyd {

using slash::Status;

class PeerThread;

struct FloydPeerEnv {
  std::string server;
  FloydContext* context;
  Floyd* floyd;
  FloydApply* apply;
  Log* log;
  
  FloydPeerEnv(const std::string _server, FloydContext* _ctx, Floyd* _floyd,
               FloydApply* _apply, Log* _log)
    : server(_server),
      context(_ctx), 
      floyd(_floyd),
      apply(_apply),
      log(_log) { }
};

class Peer {
 public:
  Peer(FloydPeerEnv env);
  ~Peer();

  int StartThread();

  // Apend Entries
  inline void AddAppendEntriesTask();
  static void DoAppendEntries(void *arg);
  inline void AddAppendEntriesTimerTask(bool right_now = false);
  static void DoAppendEntriesTimer(void *arg);
  Status AppendEntries();

  // Request Vote
  inline void AddRequestVoteTask();
  Status RequestVote();
  static void DoRequestVote(void *arg);

  void BeginLeaderShip();

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
