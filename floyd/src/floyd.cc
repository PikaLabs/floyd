#include "floyd/include/floyd.h"

#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/command.pb.h"

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
  leader_elect_timer_ = new pink::Timer(); 
  worker_ = new FloydWorker(FloydWorkerEnv(options_.local_port, 1000, this));
  apply_ = new FloydApply(FLoydApplyEnv(context_, db_));

  // peer threads
  for (auto it = options_.members.begin();
      it != options_.members.end(); it++) {
    if (!IsSelf(*iter)) {
      PeerThread* pt = new PeerThread(FloydPeerEnv(*iter, &context_, *iter, apply_));
      peers_.insert(std::pair<std::string, PeerThread*>(*iter, pt));
    }
  }
}

Floyd::~Floyd() {
  Stop();
}

bool Floyd::IsSelf(const std::string& ip_port) {
  return (ip_port == 
    slash::IpPortString(options_.local_ip, options_.local_port));
}

bool Floyd::GetLeader(std::string& ip_port) {
  auto leader_node = context_.leader_node();
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
  options_.create_if_missing = true;
  rocksdb::Status s = rocksdb::DBNemo::Open(options, options_.data_path, &db_);
  if (!s.ok()) {
    LOG_ERROR("Open db failed! path: " + options_.data_path);
    return s;
  }

  // Recover from log
  Status s = FileLog::Create(options_.log_path, log_);
  if (!s.ok()) {
    LOG_ERROR("Open file log failed! path: " + options_.log_path);
    return s;
  }
  context_.RecoverInit(log);

  // Start leader_elect_timer
  int ret;
  if ((ret = leader_elect_timer_->StartThread()) != 0) {
    LOG_ERROR("Floyd leader elect timer failed to start, ret is %d", ret);
    return Status::Corruption("failed to start leader elect timer , return " + std::to_string(ret));
  }
  bool ok = leader_elect_timer_.Schedule(options_.elect_timeout_ms,
      FLoyd::StartNewElection,
      static_cast<void*>(new LeaderElectTimerEnv(context_, peers_)));
  if (!ok) {
    LOG_ERROR("Failed to schedule leader elect timer");
    return Status::Corruption("Failed to schedule leader elect timer");
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

void Floyd::Stop() {
  delete apply_;
  for (auto& pt : peers) {
    delete pt.second;
  }
  delete worker_;
  delete leader_elect_timer_;
  delete db_;
  delete log_;
  return Status::OK();
}

void Floyd::Erase() {
  Stop();
  slash::DeleteDir(options_.data_path);
  slash::DeleteDir(options_.log_path);
  return Status::OK();
}

void Floyd::StartNewElection(void* arg) {
  LeaderElectTimerEnv* targ = static_cast<Floyd*>(arg);
  targ->context.BecomeCandidate();
  for (auto& peer : targ->peers) {
    peer.second->AddRequestVoteTask();
  }
}

void Floyd::BeginLeaderShip() {
  context_.BecomeLeader();
  for (auto& peer : peers_) {
    peer.second->BeginLeaderShip();
  }
}

void Floyd::AdvanceCommitIndex() {
  if (context_.role() != Role::kLeader) {
    return;
  }

  uint64_t commit_index = context_.commit_index();
  uint64_t new_commit_index = ULLONG_MAX;
  for (auto& iter : peers) {
    new_commit_index = std::min(iter->second->GetLastAgreeIndex(), min_commit_index);
  }
  uint64_t apply_index = context_.apply_index();
  LOG_DEBUG("AdvanceCommitIndex: new_commit_index=%lu, old commit_index_=%lu, apply_index()=%lu",
            new_commit_index, commit_index, context_.apply_index());

  if (commit_index >= new_commit_index) {
    if (commit_index > apply_index) {
      apply_->ScheduleApply();
    }
    return;
  }

  if (log_->GetEntry(new_commit_index).term() == current_term_) {
    context_.SetCommitIndex(new_commit_index);
    LOG_DEBUG("AdvanceCommitIndex: commit_index=%ld", new_commit_index);
  }
}

} // namespace floyd
