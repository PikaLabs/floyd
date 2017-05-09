#ifndef FLOYD_IMPL_H_
#define FLOYD_IMPL_H_

#include <string>

#include "floyd/include/floyd.h"
#include "floyd/include/floyd_options.h"

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

#include "pink/include/bg_thread.h"

#include "nemo-rocksdb/db_nemo_impl.h"

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
class CmdRequest;
class CmdResponse;
class CmdResponse_ServerStatus;

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
  Log* log_;
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
};

} // namespace floyd
#endif
