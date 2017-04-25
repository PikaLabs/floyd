#ifndef FLOYD_IMPL_H_
#define FLOYD_IMPL_H_

#include <string>

#include "floyd/include/floyd.h"
#include "floyd/include/floyd_options.h"
//#include "floyd/src/raft/log.h"
//#include "floyd/src/floyd_context.h"
//#include "floyd/src/floyd_worker.h"

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

#include "pink/include/bg_thread.h"

#include "nemo-rocksdb/db_nemo_impl.h"

namespace command {
class Command;
class CommandRes;
class CommandRes_RaftStageRes;
}


namespace floyd {
using slash::Status;

namespace raft {
class Log;
}

class ClientPool;
class FloydContext;
class Peer;
class FloydApply;
class FloydWorker;
class FloydWorkerConn;
struct LeaderElectTimerEnv;

typedef std::map<std::string, Peer*> PeersSet;

class FloydImpl : public Floyd {
 public:
  FloydImpl(const Options& options);
  virtual ~FloydImpl();

  virtual Status Start();

  virtual Status Write(const std::string& key, const std::string& value);
  virtual Status DirtyWrite(const std::string& key, const std::string& value);
  virtual Status Delete(const std::string& key);
  virtual Status Read(const std::string& key, std::string& value);
  virtual Status DirtyRead(const std::string& key, std::string& value);
  //Status ReadAll(std::map<std::string, std::string>& kvMap);
  //Status DirtyReadAll(std::map<std::string, std::string>& kvMap);
  //Status TryLock(const std::string& key);
  //Status UnLock(const std::string& key);

  // return true if leader has been elected
  virtual bool GetLeader(std::string& ip_port);
  virtual bool GetServerStatus(std::string& msg);
  

 private:
  //friend class Floyd;
  friend class FloydWorkerConn;
  friend class FloydWorkerHandle;
  friend class Peer;

  Options options_;
  rocksdb::DBNemo* db_;
  raft::Log* log_;
  FloydContext* context_;

  FloydWorker* worker_;
  FloydApply* apply_;
  pink::Timer* leader_elect_timer_;
  LeaderElectTimerEnv* leader_elect_env_;
  PeersSet peers_;
  ClientPool* peer_client_pool_;
  ClientPool* worker_client_pool_;

  void BeginLeaderShip();
  ClientPool* peer_client_pool() {
    return peer_client_pool_;
  }
  void AdvanceCommitIndex();
  void ResetLeaderElectTimer();

  bool IsSelf(const std::string& ip_port);
  bool HasLeader();

  uint64_t QuorumMatchIndex();

  Status DoCommand(const command::Command& cmd,
      command::CommandRes *cmd_res);
  Status ExecuteCommand(const command::Command& cmd,
      command::CommandRes *cmd_res);
  Status ExecuteDirtyCommand(const command::Command& cmd,
      command::CommandRes *cmd_res);
  void DoRequestVote(command::Command& cmd,
      command::CommandRes* cmd_res);
  void DoAppendEntries(command::Command& cmd,
      command::CommandRes* cmd_res);
  bool DoGetServerStatus(command::CommandRes_RaftStageRes* res);
  static void StartNewElection(void* arg);
  
  // No coping allowed
  FloydImpl(const FloydImpl&);
  void operator=(const FloydImpl&);
};

} // namespace floyd
#endif
