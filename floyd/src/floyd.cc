#include "floyd/include/floyd.h"

#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_worker.h"
#include "floyd/src/raft/log.h"
#include "floyd/src/raft/memory_log.h"
#include "floyd/src/raft/file_log.h"
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/logger.h"
#include "floyd/src/floyd_rpc.h"
#include "floyd/src/raft/file_log.h"

#include "slash/include/slash_string.h"
#include "slash/include/env.h"

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

  log_ = new FileLog(options_.log_path)

  leader_elect_env_ = new LeaderElectTimerEnv(context_, &peers_);
  leader_elect_timer_ = new pink::Timer(options_.elect_timeout_ms,
      Floyd::StartNewElection,
      static_cast<void*>(leader_elect_env_));
  worker_ = new FloydWorker(FloydWorkerEnv(options_.local_port, 1000, this));
  apply_ = new FloydApply(FloydApplyEnv(context_, db_, log_));

  // peer threads
  for (auto iter = options_.members.begin();
      iter != options_.members.end(); iter++) {
    if (!IsSelf(*iter)) {
      Peer* pt = new Peer(FloydPeerEnv(*iter, context_, this,
            apply_, log_));
      peers_.insert(std::pair<std::string, Peer*>(*iter, pt));
    }
  }

  peer_rpc_client_ = new RpcClient();
  context_ = new FloydContext(options_, log_);
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
    LOG_ERROR("Open db failed! path: " + options_.data_path);
    return Status::Corruption("Open DB failed, ", + s.ToString());
  }

  // Recover from log
  //Status s = FileLog::Create(options_.log_path, log_);
  //if (!s.ok()) {
  //  LOG_ERROR("Open file log failed! path: " + options_.log_path);
  //  return s;
  //}
  //context_->RecoverInit(log_);

  //log_ = new raft::FileLog(options_.log_path);
  context_->RecoverInit();

  // Start leader_elect_timer
  int ret;
  if (!leader_elect_timer_->Start()) {
    LOG_ERROR("Floyd leader elect timer failed to start");
    return Status::Corruption("failed to start leader elect timer");
  }

  // Start worker thread
  if ((ret = worker_->Start()) != 0) {
    LOG_ERROR("Floyd worker thread failed to start, ret is %d", ret);
    return Status::Corruption("failed to start worker, return " + std::to_string(ret));
  }
  
  // Start peer thread
  for (auto& pt : peers_) {
    if (ret = pt.second->StartThread() != 0) {
      LOG_ERROR("Floyd peer thread to %s failed to start, ret is %d",
          pt.first.c_str(), ret);
      return Status::Corruption("failed to start peer thread to " + pt.first);
    }
  }

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

void Floyd::BeginLeaderShip() {
  context_->BecomeLeader();
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

  uint64_t commit_index = context_->commit_index();
  uint64_t new_commit_index = QuorumMatchIndex();
  uint64_t apply_index = context_->apply_index();
  LOG_DEBUG("AdvanceCommitIndex: new_commit_index=%lu, old commit_index_=%lu, apply_index()=%lu",
            new_commit_index, commit_index, context_->apply_index());

  if (commit_index >= new_commit_index) {
    if (commit_index > apply_index) {
      apply_->ScheduleApply();
    }
    return;
  }

  if (log_->GetEntry(new_commit_index).term() == context_->current_term()) {
    context_->SetCommitIndex(new_commit_index);
    LOG_DEBUG("AdvanceCommitIndex: commit_index=%ld", new_commit_index);
  }
}

} // namespace floyd
