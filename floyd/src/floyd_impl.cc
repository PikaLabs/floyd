#include "floyd/src/floyd_impl.h"

#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_apply.h"
#include "floyd/src/floyd_worker.h"
#include "floyd/src/file_log.h"
#include "floyd/src/floyd_peer_thread.h"
#include "floyd/src/floyd_primary_thread.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/logger.h"


#include "slash/include/env.h"
#include "slash/include/slash_string.h"

namespace floyd {

FloydImpl::FloydImpl(const Options& options)
  : options_(options),
  db_(NULL) {
  // TODO (anan) set timeout and retry
  peer_client_pool_ = new ClientPool();
  worker_client_pool_ = new ClientPool();
}

FloydImpl::~FloydImpl() {
  delete primary_;
  delete apply_;
  for (auto& pt : peers_) {
    delete pt.second;
  }
  delete worker_;
  delete db_;
  delete log_;
  delete context_;
  delete peer_client_pool_;
  delete worker_client_pool_;
}

bool FloydImpl::IsSelf(const std::string& ip_port) {
  return (ip_port == 
    slash::IpPortString(options_.local_ip, options_.local_port));
}

bool FloydImpl::GetLeader(std::string& ip_port) {
  auto leader_node = context_->leader_node();
  if (leader_node.first.empty() || leader_node.second == 0) {
    return false;
  }
  ip_port = slash::IpPortString(leader_node.first, leader_node.second);
  return true;
}

Status FloydImpl::Start() {
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

  // Recover Context
  log_ = new FileLog(options_.log_path);
  context_ = new FloydContext(options_, log_);
  context_->RecoverInit();

  // Create Apply threads
  apply_ = new FloydApply(context_, db_, log_);

  // TODO(annan) peers and primary refer to each other
  // Create PrimaryThread before Peers
  primary_ = new FloydPrimary(context_, apply_);

  // Create peer threads
  for (auto iter = options_.members.begin();
      iter != options_.members.end(); iter++) {
    if (!IsSelf(*iter)) {
      Peer* pt = new Peer(*iter, context_, primary_, log_, peer_client_pool_);
      peers_.insert(std::pair<std::string, Peer*>(*iter, pt));
    }
  }
  
  // Start peer thread
  int ret;
  for (auto& pt : peers_) {
    if ((ret = pt.second->StartThread()) != 0) {
      LOG_ERROR("FloydImpl peer thread to %s failed to start, ret is %d",
          pt.first.c_str(), ret);
      return Status::Corruption("failed to start peer thread to " + pt.first);
    }
  }

  // Start worker thread after Peers, because WorkerHandle will check peers
  worker_ = new FloydWorker(options_.local_port, 1000, this);
  if ((ret = worker_->Start()) != 0) {
    LOG_ERROR("FloydImpl worker thread failed to start, ret is %d", ret);
    return Status::Corruption("failed to start worker, return " + std::to_string(ret));
  }

  // Set and Start PrimaryThread
  primary_->SetPeers(&peers_);
  if ((ret = primary_->Start()) != 0) {
    LOG_ERROR("FloydImpl primary thread failed to start, ret is %d", ret);
    return Status::Corruption("failed to start primary thread, return " + std::to_string(ret));
  }
  primary_->AddTask(kCheckElectLeader);

  options_.Dump();
  LOG_DEBUG("FloydImpl started");
  return Status::OK();
}

Status Floyd::Open(const Options& options, Floyd** floyd) {
  *floyd = new FloydImpl(options);
  return Status::OK();
}

Floyd::~Floyd() { }

} // namespace floyd
