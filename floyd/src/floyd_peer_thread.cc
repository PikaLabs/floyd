#include "floyd/src/floyd_peer_thread.h"

#include <climits>
#include <google/protobuf/text_format.h>
#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_rpc.h"
#include "floyd/src/raft/log.h"
#include "floyd/src/command.pb.h"
#include "floyd/src/logger.h"

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

Peer::Peer(FloydPeerEnv env)
  : env_(env),
    next_index_(1),
    match_index_(0) {
}

int Peer::StartThread() {
  bg_thread_.set_thread_name("FloydPr" + env_.server.substr(env_.server.find(':')));
  return bg_thread_.StartThread();
}

Peer::~Peer() {
  LOG_INFO("Peer(%s) exit!!!", env_.server.c_str());
  //bg_thread_.set_runing(false);
}

void Peer::set_next_index(uint64_t next_index) {
  next_index_ = next_index;
}

uint64_t Peer::get_next_index() {
  return next_index_;
}

void Peer::AddRequestVoteTask() {
  bg_thread_.Schedule(DoRequestVote, this);
}

void Peer::DoRequestVote(void *arg) {
  Peer *peer = static_cast<Peer*>(arg);
  Status result = peer->RequestVote();
  if (!result.ok()) {
    LOG_ERROR("Peer(%s) failed to RequestVote caz %s.", peer->env_.server.c_str(), result.ToString().c_str());
  }
}

Status Peer::RequestVote() {
  if (env_.context->role() != Role::kCandidate) {
    return Status::OK();
  }
  
  // TODO (anan) log->getEntry() need lock
  uint64_t last_log_term = 0;
  uint64_t last_log_index = env_.context->log()->GetLastLogIndex();
  if (last_log_index != 0) {
    last_log_term = env_.context->log()->GetEntry(last_log_index).term();
  }

  command::Command req;
  req.set_type(command::Command::RaftVote);
  floyd::raft::RequestVote* rqv = req.mutable_rqv();
  rqv->set_ip(env_.context->local_ip());
  rqv->set_port(env_.context->local_port());
  rqv->set_term(env_.context->current_term());
  rqv->set_last_log_term(last_log_term);
  rqv->set_last_log_index(last_log_index);

#if (LOG_LEVEL != LEVEL_NONE)
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(req, &text_format);
  LOG_DEBUG("Send RequestVote to %s, message :\n%s", env_.server.c_str(), text_format.c_str());
#endif

  command::CommandRes res;
  Status result = env_.floyd->peer_rpc_client()->SendRequest(env_.server, req, &res);
  
  if (!result.ok()) {
    LOG_DEBUG("RequestVote to %s failed %s", env_.server.c_str(), result.ToString().c_str());
    return result;
  }

#if (LOG_LEVEL != LEVEL_NONE)
  google::protobuf::TextFormat::PrintToString(res, &text_format);
  LOG_DEBUG("Recv RequestVote from %s, message :\n%s", env_.server.c_str(), text_format.c_str());
#endif

  uint64_t res_term = res.rsv().term();
  if (res_term > env_.context->current_term()) {
    //TODO(anan) every peer
    env_.context->BecomeFollower(res_term);
  } else {
    if (res.rsv().granted()) {
      if (env_.context->VoteAndCheck(res_term)) {
        //env_.context->BecomeLeader();
        env_.floyd->BeginLeaderShip();  
      }
    } else {
      LOG_DEBUG("Vote request denied by %s", env_.server.c_str());
    }
  }

  return Status::OK();
}

void Peer::BeginLeaderShip() {
  {
    slash::MutexLock l(&mu_);
    next_index_ = env_.log->GetLastLogIndex() + 1;
    match_index_ = 0;
  }

  AddAppendEntriesTimerTask(true);
}

void Peer::AddAppendEntriesTask() {
  bg_thread_.Schedule(DoAppendEntries, this);
}

void Peer::DoAppendEntries(void *arg) {
  Peer* peer = static_cast<Peer*>(arg);
  Status result = peer->AppendEntries();
  if (!result.ok()) {
    LOG_ERROR("Peer(%s) failed to AppendEntries caz %s.", peer->env_.server.c_str(), result.ToString().c_str());
  }
}

void Peer::AddAppendEntriesTimerTask(bool right_now) {
  bg_thread_.DelaySchedule(right_now ? slash::NowMicros()
                           : slash::NowMicros() + env_.context->heartbeat_us(),
                           DoAppendEntriesTimer, this);
}

void Peer::DoAppendEntriesTimer(void *arg) {
  Peer* peer = static_cast<Peer*>(arg);
  Status result = peer->AppendEntries();
  if (!result.ok()) {
    LOG_ERROR("Peer(%s) failed to AppendEntriesTimer caz %s.", peer->env_.server.c_str(), result.ToString().c_str());
  }
  peer->AddAppendEntriesTimerTask();
}

//bool Peer::HaveVote() { return have_vote_; }

uint64_t Peer::GetMatchIndex() {
  slash::MutexLock l(&mu_);
  return match_index_;
}

Status Peer::AppendEntries() {
  uint64_t last_log_index = env_.log->GetLastLogIndex();
  uint64_t prev_log_index = next_index_ - 1;
  if (prev_log_index > last_log_index) {
    return Status::InvalidArgument("prev_Log_index > last_log_index");
  }

  uint64_t prev_log_term = 0;
  if (prev_log_index != 0) {
    prev_log_term = env_.log->GetEntry(prev_log_index).term();
  }

  command::Command req;
  floyd::raft::AppendEntriesRequest* aerq = req.mutable_aerq();
  req.set_type(command::Command::RaftAppendEntries);
  aerq->set_ip(env_.context->local_ip());
  aerq->set_port(env_.context->local_port());
  aerq->set_term(env_.context->current_term());
  aerq->set_prev_log_index(prev_log_index);
  aerq->set_prev_log_term(prev_log_term);

  uint64_t num_entries = 0;
  for (uint64_t index = next_index_; index <= last_log_index; ++index) {
    Log::Entry& entry = env_.log->GetEntry(index);
    *aerq->add_entries() = entry;
    uint64_t request_size = aerq->ByteSize();
    if (request_size < env_.context->append_entries_size_once() ||
        num_entries == 0)
      ++num_entries;
    else
      aerq->mutable_entries()->RemoveLast();
  }
  aerq->set_commit_index(
      std::min(env_.context->commit_index(), prev_log_index + num_entries));

#if (LOG_LEVEL != LEVEL_NONE)
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(req, &text_format);
  LOG_DEBUG("AppendEntry Send to %s, message :\n%s", env_.server.c_str(), text_format.c_str());
#endif

  command::CommandRes res;
  Status result = env_.floyd->peer_rpc_client()->SendRequest(env_.server, req, &res);
  
  if (!result.ok()) {
    LOG_DEBUG("AppendEntry to %s failed %s", env_.server.c_str(), result.ToString().c_str());
    return result;
  }
#if (LOG_LEVEL != LEVEL_NONE)
  google::protobuf::TextFormat::PrintToString(res, &text_format);
  LOG_DEBUG("AppendEntry Receive from %s, message :\n%s", env_.server.c_str(), text_format.c_str());
#endif

  uint64_t res_term = res.aers().term();
  if (res_term > env_.context->current_term()) {
    //TODO(anan) every peer
    env_.context->BecomeFollower(res_term);
  } else {
    if (res.aers().status()) {
      match_index_ = prev_log_index + num_entries;
      //TODO(anan) 
      env_.floyd->AdvanceCommitIndex();
      next_index_ = match_index_ + 1;
    } else {
      if (next_index_ > 1) --next_index_;
    }
  }
  return Status::OK();
}

} // namespace floyd
