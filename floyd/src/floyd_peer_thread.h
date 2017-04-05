#ifndef FLOYD_PEER_THREAD_H_
#define FLOYD_PEER_THREAD_H_

#include "slash/include/slash_status.h"
//#include "slash/include/slash_slice.h"

#include "pink/include/bg_thread.h"
//#include "pink/include/pb_conn.h"

namespace floyd {

using slash::Status;

class PeerThread;

class Peer {
 public:
  Peer(const std::string server);
  ~Peer();

  struct AppendArg {
    Peer* peer;
    bool again;

    AppendArg(Peer* _peer, bool _again)
      : peer(_peer), again(_again) {}
  };
  inline void AddAppendEntriesTask();
  inline void AddAppendEntriesTimerTask();
  Status AppendEntries();
  static void DoAppendEntries(void *arg);

  inline void AddRequestVoteTask();
  bool RequestVote();
  static void DoRequestVote(void *arg);

  //bool HaveVote();
  uint64_t GetLastAgreeIndex();
  //void BeginRequestVote();
  void BeginLeaderShip();

  void set_next_index(uint64_t next_index);
  uint64_t get_next_index();

 private:
  //RaftConsensus *raft_;

  pink::BGThread bg_thread_;
  std::string server_;
 // bool have_vote_;
  bool vote_done_;
  uint64_t next_index_;
  uint64_t last_agree_index_;
  uint64_t next_heartbeat_us_;

  // No copying allowed
  Peer(const Peer&);
  void operator=(const Peer&);
};

} // namespace floyd
#endif
