#ifndef FLOYD_IMPL_H_
#define FLOYD_IMPL_H_

#include <string>
#include <map>

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "pink/include/bg_thread.h"

#include "floyd/include/floyd.h"
#include "floyd/include/floyd_options.h"
#include "floyd/src/raft_log.h"


namespace floyd {
using slash::Status;

class Log;
class ClientPool;
class FloydContext;
class Peer;
class FloydPrimary;
class FloydApply;
class FloydWorker;
class FloydWorkerConn;
class Logger;
class CmdRequest;
class CmdResponse;
class CmdResponse_ServerStatus;

typedef std::map<std::string, Peer*> PeersSet;

class FloydImpl : public Floyd {
 public:
  FloydImpl(const Options& options);
  virtual ~FloydImpl();

  Status Init();

  virtual Status Write(const std::string& key, const std::string& value);
  virtual Status DirtyWrite(const std::string& key, const std::string& value);
  virtual Status Delete(const std::string& key);
  virtual Status Read(const std::string& key, std::string& value);
  virtual Status DirtyRead(const std::string& key, std::string& value);

  // return true if leader has been elected
  virtual bool GetLeader(std::string& ip_port);
  virtual bool GetLeader(std::string* ip, int* port);
  virtual bool GetAllNodes(std::vector<std::string>& nodes);
  virtual bool GetServerStatus(std::string& msg);
  
  // log level can be modified
  virtual void set_log_level(const int log_level);

 private:
  //friend class Floyd;
  friend class FloydWorkerConn;
  friend class FloydWorkerHandle;
  friend class Peer;

  Options options_;
  rocksdb::DB* db_;
  // raft log
  RaftLog* raft_log_;
  // debug log used for ouput to file
  Logger* info_log_;
  FloydContext* context_;

  FloydWorker* worker_;
  FloydApply* apply_;
  FloydPrimary* primary_;
  PeersSet peers_;
  ClientPool* peer_client_pool_;
  ClientPool* worker_client_pool_;

  bool IsSelf(const std::string& ip_port);
  bool HasLeader();

  Status DoCommand(const CmdRequest& cmd, CmdResponse *cmd_res);
  Status ExecuteCommand(const CmdRequest& cmd, CmdResponse *cmd_res);
  Status ExecuteDirtyCommand(const CmdRequest& cmd, CmdResponse *cmd_res);
  void DoRequestVote(CmdRequest& cmd, CmdResponse* cmd_res);
  void DoAppendEntries(CmdRequest& cmd, CmdResponse* cmd_res);
  bool DoGetServerStatus(CmdResponse_ServerStatus* res);
  
  // No coping allowed
  FloydImpl(const FloydImpl&);
  void operator=(const FloydImpl&);
}; // FloydImpl

} // namespace floyd
#endif  // 
