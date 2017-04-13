#include "floyd/include/floyd.h"

#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_worker.h"
#include "floyd/src/raft/log.h"
#include "floyd/src/raft/file_log.h"
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/floyd_rpc.h"
#include "floyd/src/logger.h"


#include "slash/include/env.h"
#include "slash/include/slash_string.h"

namespace floyd {


struct LeaderElectTimerEnv {
  FloydContext* context;
  PeersSet* peers;
  LeaderElectTimerEnv(FloydContext* c, PeersSet* s)
    : context(c),
    peers(s) {}
};

Floyd::Floyd(const Options& options)
  : options_(options),
  db_(NULL) {
  peer_rpc_client_ = new RpcClient();
  worker_rpc_client_ = new RpcClient();
}

Floyd::~Floyd() {
  delete apply_;
  for (auto& pt : peers_) {
    delete pt.second;
  }
  delete worker_;
  delete leader_elect_timer_;
  delete leader_elect_env_;
  delete db_;
  delete log_;
  delete context_;
  delete peer_rpc_client_;
  delete worker_rpc_client_;
}

bool Floyd::IsSelf(const std::string& ip_port) {
  return (ip_port == 
    slash::IpPortString(options_.local_ip, options_.local_port));
}

bool Floyd::GetLeader(std::string& ip_port) {
  auto leader_node = context_->leader_node();
  if (leader_node.first.empty() || leader_node.second == 0) {
    return false;
  }
  ip_port = slash::IpPortString(leader_node.first, leader_node.second);
  return true;
}

Status Floyd::Start() {
  LOG_DEBUG("Start: floyd starting...");

  slash::CreatePath(options_.log_path);
  slash::CreatePath(options_.data_path);

  // Create DB
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DBNemo::Open(options, options_.data_path, &db_);
  if (!s.ok()) {
    LOG_ERROR("Open db failed! path: %s", options_.data_path.c_str());
    return Status::Corruption("Open DB failed, " + s.ToString());
  }

  // Recover from log
  //Status s = FileLog::Create(options_.log_path, log_);
  //if (!s.ok()) {
  //  LOG_ERROR("Open file log failed! path: " + options_.log_path);
  //  return s;
  //}

  log_ = new raft::FileLog(options_.log_path);
  context_ = new FloydContext(options_, log_);

  context_->RecoverInit();


  // TODO Start Apply
  apply_ = new FloydApply(FloydApplyEnv(context_, db_, log_));

  // Start leader_elect_timer
  int ret;
  leader_elect_env_ = new LeaderElectTimerEnv(context_, &peers_);
  leader_elect_timer_ = new pink::Timer(options_.elect_timeout_ms,
      Floyd::StartNewElection,
      static_cast<void*>(leader_elect_env_),
      3 * options_.elect_timeout_ms);
  LOG_DEBUG("First leader elect will in %lums.", leader_elect_timer_->RemainTime());

  if (!leader_elect_timer_->Start()) {
    LOG_ERROR("Floyd leader elect timer failed to start");
    return Status::Corruption("failed to start leader elect timer");
  }

  // Start worker thread
  worker_ = new FloydWorker(FloydWorkerEnv(options_.local_port, 1000, this));
  if ((ret = worker_->Start()) != 0) {
    LOG_ERROR("Floyd worker thread failed to start, ret is %d", ret);
    return Status::Corruption("failed to start worker, return " + std::to_string(ret));
  }

  // Create peer threads
  for (auto iter = options_.members.begin();
      iter != options_.members.end(); iter++) {
    if (!IsSelf(*iter)) {
      Peer* pt = new Peer(FloydPeerEnv(*iter, context_, this,
            apply_, log_));
      peers_.insert(std::pair<std::string, Peer*>(*iter, pt));
    }
  }
  
  // Start peer thread
  for (auto& pt : peers_) {
    if ((ret = pt.second->StartThread()) != 0) {
      LOG_ERROR("Floyd peer thread to %s failed to start, ret is %d",
          pt.first.c_str(), ret);
      return Status::Corruption("failed to start peer thread to " + pt.first);
    }
  }

  options_.Dump();
  LOG_DEBUG("Floyd started");
  return Status::OK();
}

void Floyd::StartNewElection(void* arg) {
  LeaderElectTimerEnv* targ = static_cast<LeaderElectTimerEnv*>(arg);
  targ->context->BecomeCandidate();
  for (auto& peer : *(targ->peers)) {
    peer.second->AddRequestVoteTask();
  }
}

// TODO(anan) many peers may call this; maybe critical section
void Floyd::BeginLeaderShip() {
  LOG_DEBUG("Floyd::BeginLeaderShip");
  context_->BecomeLeader();
  leader_elect_timer_->Stop();
  for (auto& peer : peers_) {
    peer.second->BeginLeaderShip();
  }
}

uint64_t Floyd::QuorumMatchIndex() {
  //if (peers_.empty()) return last_synced_index_;
  std::vector<uint64_t> values;
  for (auto& iter : peers_) {
    values.push_back(iter.second->GetMatchIndex());
  }
  std::sort(values.begin(), values.end());
  return values.at(values.size() / 2);
}

void Floyd::AdvanceCommitIndex() {
  if (context_->role() != Role::kLeader) {
    return;
  }

  uint64_t new_commit_index = QuorumMatchIndex();
  if (context_->AdvanceCommitIndex(new_commit_index)) {
      apply_->ScheduleApply();
  }
}

void Floyd::ResetLeaderElectTimer() {
  leader_elect_timer_->Reset();
}

} // namespace floyd
