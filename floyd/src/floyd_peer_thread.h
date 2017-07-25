// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef FLOYD_SRC_FLOYD_PEER_THREAD_H_
#define FLOYD_SRC_FLOYD_PEER_THREAD_H_


#include <string>

#include "slash/include/slash_status.h"
#include "pink/include/bg_thread.h"

#include "floyd/src/floyd_context.h"


namespace floyd {

using slash::Status;

class RaftMeta;
class FloydPrimary;
class RaftLog;
class ClientPool;


class Peer {
 public:
  Peer(std::string server, FloydContext* context, FloydPrimary* primary, RaftLog* raft_log, 
    ClientPool* pool, const Options& options, Logger* info_log);
  ~Peer();

  int StartThread();

  // Apend Entries
  // call by other thread, put job to peer_thread's bg_thread_
  void AddAppendEntriesTask();
  void AddRequestVoteTask();

  /*
   * the two main RPC call in raft consensus protocol is here
   * AppendEntriesRPC
   * RequestVoteRPC
   * the response to these two RPC at floyd_impl.h
   */
  static void AppendEntriesRPCWrapper(void *arg);
  Status AppendEntriesRPC();
  // Request Vote
  static void RequestVoteRPCWrapper(void *arg);
  Status RequestVoteRPC();

  uint64_t GetMatchIndex();

  void set_next_index(const uint64_t next_index) {
    next_index_ = next_index;
  }
  uint64_t next_index() {
    return next_index_;
  }

  uint64_t match_index() {
    return match_index_;
  }
  typedef std::map<std::string, Peer*> PeersSet;

  void set_peers(PeersSet* peers) {
    peers_ = peers;
  }

 private:

  bool VoteAndCheck(uint64_t vote_term);
  uint64_t QuorumMatchIndex();
  void AdvanceLeaderCommitIndex();

  Logger* info_log_;
  std::string server_;
  FloydContext* context_;
  FloydPrimary* primary_;
  RaftMeta* raft_meta_;
  RaftLog* raft_log_;
  ClientPool* pool_;
  PeersSet* peers_;

  Options options_;

  std::atomic<uint64_t> next_index_;
  std::atomic<uint64_t> match_index_;

  pink::BGThread bg_thread_;

  // No copying allowed
  Peer(const Peer&);
  void operator=(const Peer&);
};

} // namespace floyd
#endif   // FLOYD_SRC_FLOYD_PEER_THREAD_H_
