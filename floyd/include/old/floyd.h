#ifndef FLOYD_H_
#define FLOYD_H_

#include "floyd_define.h"
#include "floyd_options.h"
#include "floyd_mutex.h"
//#include "floyd_db.h"

#include "raft/raft.h"

namespace floyd {

class LeveldbBackend;
class NodeInfo;
//class FloydMetaThread;
class FloydWorkerThread;
//class FloydHeartBeatThread;

class Floyd {
 public:
  Floyd(const Options& options);
  virtual ~Floyd();

  Status Start();
  Status Stop();

  // nodes info
  static Mutex nodes_mutex;
  static std::vector<NodeInfo*> nodes_info;
  static LeveldbBackend* db;
  static floyd::raft::RaftConsensus* raft_;

  Status ChaseRaftLog(floyd::raft::RaftConsensus* raft_con);
  Status DirtyRead(const std::string& key, std::string& value);
  Status DirtyReadAll(std::map<std::string, std::string>& kvMap);
  Status DirtyWrite(const std::string& key, const std::string& value);
  Status Write(const std::string& key, const std::string& value);
  Status Read(const std::string& key, std::string& value);
  Status ReadAll(std::map<std::string, std::string>& kvMap);
  Status TryLock(const std::string& key);
  Status UnLock(const std::string& key);
  Status Erase();
  Status Delete(const std::string& key);

  // return true if leader has been elected
  bool GetLeader(std::string& ip, int& port);
  void GetAllNodes(std::vector<std::string> &nodes);
  bool GetServerStatus(std::string& msg);

 private:

  const Options options_;

  //FloydMetaThread* meta_thread_;
  FloydWorkerThread* worker_thread_;
  //FloydHeartBeatThread* heartbeat_;

  bool IsLeader();

  NodeInfo* GetLeaderInfo();
};

} // namespace floyd
#endif
