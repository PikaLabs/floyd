#ifndef __FLOYD_H__
#define __FLOYD_H__

#include "floyd_define.h"
#include "floyd_util.h"
#include "floyd_mutex.h"
#include "floyd_hb.h"
#include "floyd_meta.h"
#include "floyd_worker.h"
#include "floyd_db.h"
#include "raft/raft.h"
#include "floyd_rpc.h"

namespace floyd {

class Floyd {
 public:
  Floyd(const Options& options);
  virtual ~Floyd();

  Status Start();

  /*
   * Normal start can't handle read/write requests until a leader is elected.
   * If you are not building a cluster,time spent for election will be a
   * waste.Use SingleStart then.
   * ATTENTION! DO NOT USE IT IN WHENING CONNECTIONG TO A CLUSTER.
   */
  Status SingleStart();

  // nodes info
  static Mutex nodes_mutex;
  static std::vector<NodeInfo*> nodes_info;
  static DbBackend* db;
  static floyd::raft::RaftConsensus* raft_con;
  Status ChaseRaftLog(floyd::raft::RaftConsensus* raft_con);
  Status DirtyRead(const std::string& key, std::string& value);
  Status DirtyReadAll(std::map<std::string, std::string>& kvMap);
  Status DirtyWrite(const std::string& key, const std::string& value);
  Status Write(const std::string& key, const std::string& value);
  Status Read(const std::string& key, std::string& value);
  Status ReadAll(std::map<std::string, std::string>& kvMap);
  Status TryLock(const std::string& key);
  Status UnLock(const std::string& key);
  Status Stop();
  Status Erase();
  Status Delete(const std::string& key);

  // return true if leader has been elected
  bool GetLeader(std::string& ip, int& port);
  void GetAllNodes(std::vector<std::string> &nodes);

 private:
  const Options options_;
  // FloydLiveThread
  FloydMetaThread* floydmeta_;
  // FloydWorkerThread* worker_;
  FloydWorkerThread* floydworker_;
  // HeartbeatThread
  FloydHeartBeatThread* heartbeat_;
  bool IsLeader();

  // Status UpHoldWorkerCliConn(NodeInfo *ni);
  NodeInfo* GetLeaderInfo();
  Status AddNodeFromMetaRes(meta::MetaRes* meta_res,
                            std::vector<NodeInfo*>* nis);
  Status FetchRemoteMap(const std::string& ip, const int port,
                        std::vector<NodeInfo*>* nis);
  Status MergeRemoteMap(const std::string& ip, const int port);
};
}
#endif
