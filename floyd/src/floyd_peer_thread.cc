#include "floyd/src/floyd_peer_thread.h"

#include "floyd/include/floyd.h"
#include "floyd/include/floyd_util.h"
#include "floyd/include/command.pb.h"

#include <climits>
#include <google/protobuf/text_format.h>

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

Peer::Peer(const std::string& server)
  : server_(server),
    //have_vote_(false),
    vote_done_(false),
    next_index_(1),
    last_agree_index_(0) {
  next_heartbeat_us_ = slash::NowMicros();
  if (bg_thread_.StartThread() != 0) {
    LOG_ERROR("failed to Start Peer(%s)!!!", server_.c_str());
    // TODO(anan) should exit
    //exit(-1);
  }
}

Peer::~Peer() {
  LOG_INFO("Peer(%s) exit!!!", server_.c_str());
  //bg_thread_.set_runing(false);
}

/*
void* Peer::ThreadMain() {
  uint64_t when = slash::NowMicros();

  slash::MutexLock l(&raft_->mutex_);
  while (running()) {
    switch (raft_->state_) {
      case State::FOLLOWER: {
        LOG_DEBUG("Peer(%s) I'm follower", server_.c_str());
        when = ULLONG_MAX;
        break;
      }
      case State::CANDIDATE: {
        LOG_DEBUG("Peer(%s) I'm candidate, vote_done_(%s)",
                  server_.c_str(), vote_done_ ? "true" : "false");
        if (!vote_done_) {
          Status s = RequestVote();
          if (s.ok()) {
            continue;
          }
        } else {
          when = ULLONG_MAX;
        }
        break;
      }
      case State::LEADER: {
        bool heartbeat_timeout = next_heartbeat_us_ < slash::NowMicros();
        LOG_DEBUG("Peer(%s) I'm leader, LastAgreeIndex(%d) and LastLogIndex(%d)"
                  " heartbeat_timout(%s)",
                  server_.c_str(), GetLastAgreeIndex(),
                  raft_->log_->GetLastLogIndex(), heartbeat_timeout ? "true" : "false");
        if (GetLastAgreeIndex() < raft_->log_->GetLastLogIndex()
            || heartbeat_timeout) {
          if (!AppendEntries()) {
            //ni_->UpHoldWorkerCliConn(true);
          }
          next_heartbeat_us_ = slash::NowMicros() + raft_->period_;
        }
        when = next_heartbeat_us_;
        break;
      }
    }

    int ret = raft_->state_changed_.WaitUntil(when);
    LOG_DEBUG("Peer::ThreadMain back from wait  ret=%d", ret);
    if (ret != 0 && ret != ETIMEDOUT) {
      LOG_DEBUG("WaitUntil failed! error: %d, %s", ret, strerror(ret));
    }
  }

  return NULL;
}
*/

void Peer::set_next_index(uint64_t next_index) {
  next_index_ = next_index;
}

uint64_t Peer::get_next_index() { return next_index_; }


inline void Peer::AddRequestVoteTask() {
  bg_thread_.Schedule(DoRequestVote, this);
}

static void Peer::DoRequestVote(void *arg) {
  Peer *peer = static_cast<Peer*>(arg);
  Status result = peer->RequestVote();
  if (!result.ok()) {
    LOG_ERROR("Peer(%s) failed to RequestVote caz %s.", server_.c_str(), result.ToString());
  }
}

Status Peer::RequestVote() {
  if (vote_done_ || state != State::Candidate) {
    return Stauts::OK();
  }

  command::Command req;
  req.set_type(command::Command::RaftVote);
  floyd::raft::RequestVote* rqv = cmd.mutable_rqv();
  rqv->set_ip(raft_->options_.local_ip);
  rqv->set_port(raft_->options_.local_port);
  rqv->set_term(raft_->current_term_);
  rqv->set_last_log_term(raft_->GetLastLogTerm());
  rqv->set_last_log_index(raft_->log_->GetLastLogIndex());

#if (LOG_LEVEL != LEVEL_NONE)
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(req, &text_format);
  LOG_DEBUG("Send RequestVote to %s, message :\n%s", server_.c_str(), text_format.c_str());
#endif

  command::CommandRes res;
  Status result = rpc_client_->SendRequest(server, req, res);
  
  if (!result.ok()) {
    LOG_DEBUG("RequestVote to %s failed %s", server_.c_str(), result.ToString().c_str());
    return result;
  }

#if (LOG_LEVEL != LEVEL_NONE)
  google::protobuf::TextFormat::PrintToString(res, &text_format);
  LOG_DEBUG("Recv RequestVote from %s, message :\n%s", server_.c_str(), text_format.c_str());
#endif

  if (res.rsv().term() > raft_->current_term_) {
    raft_->StepDown(res.rsv().term());
  } else {
    vote_done_ = true;
    if (res.rsv().granted()) {
      //have_vote_ = true;
      if (raft_->voted > (peers_.size() + 1) / 2)
        raft_->BecomeLeader();
    } else {
      LOG_DEBUG("Vote request denied by %s", server_.c_str());
    }
  }

  return Status::OK();
}

//void Peer::BeginRequestVote() {
//  LOG_DEBUG("Peer(%s)::BeginRequestVote", server_.c_str());
//  vote_done_ = false;
//  //have_vote_ = false;
//}

void Peer::BeginLeaderShip() {
  next_index_ = raft_->log_->GetLastLogIndex() + 1;
  last_agree_index_ = 0;

  AddAppenEntriesTimerTask();
  //AppendArg* arg = new AppendArg(this, true);
  //bg_thread_.DelaySchedule(slash::NowMicros() + period_, DoAppendEntries, arg);
}

static void Peer::DoAppendEntries(void *arg) {
  AppendArg *parg = static_cast<AppendArg*>(arg);
  Peer *peer = parg->peer;
  Status result = peer->AppendEntries(parg->again);
  if (!result.ok()) {
    LOG_ERROR("Peer(%s) failed to AppendEntries caz %s.", server_.c_str(), result.ToString());
  }
  if (parg->again) {
    AddAppendEntriesTimerTask();
  }
  delete parg;
}

void Peer::AddAppendEntriesTimerTask() {
  AppendArg* arg = new AppendArg(this, true);
  bg_thread_.DelaySchedule(slash::NowMicros() + period_, DoAppendEntries, arg);
}

void Peer::AddAppendEntriesTask() {
  AppendArg* arg = new AppendArg(this, false);
  bg_thread_.Schedule(DoAppendEntries, arg);
}

//bool Peer::HaveVote() { return have_vote_; }

uint64_t Peer::GetLastAgreeIndex() {
  return last_agree_index_;
}

Status Peer::AppendEntries() {
  uint64_t last_log_index = raft_->log_->GetLastLogIndex();
  uint64_t prev_log_index = next_index_ - 1;
  if (prev_log_index > last_log_index) {
    return Staus::InvalidArgument("prev_Log_index > last_log_index");
  }

  assert(prev_log_index <= last_log_index);

  uint64_t prev_log_term;
  if (prev_log_index == 0) {
    prev_log_term = 0;
  } else {
    prev_log_term = raft_->log_->GetEntry(prev_log_index).term();
  }

  command::Command cmd;
  floyd::raft::AppendEntriesRequest* aerq = cmd.mutable_aerq();
  cmd.set_type(command::Command::RaftAppendEntries);
  aerq->set_ip(raft_->options_.local_ip);
  aerq->set_port(raft_->options_.local_port);
  aerq->set_term(raft_->current_term_);
  aerq->set_prev_log_index(prev_log_index);
  aerq->set_prev_log_term(prev_log_term);

  uint64_t num_entries = 0;
  for (uint64_t index = next_index_; index <= last_log_index; ++index) {
    Log::Entry& entry = raft_->log_->GetEntry(index);
    *aerq->add_entries() = entry;
    uint64_t request_size = aerq->ByteSize();
    if (request_size < raft_->options_.append_entries_size_once ||
        num_entries == 0)
      ++num_entries;
    else
      aerq->mutable_entries()->RemoveLast();
  }
  aerq->set_commit_index(
      std::min(raft_->commit_index_, prev_log_index + num_entries));

#if (LOG_LEVEL != LEVEL_NONE)
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(cmd, &text_format);
  LOG_DEBUG("AppendEntry Send to %s, message :\n%s", server_.c_str(), text_format.c_str());
#endif

  command::CommandRes res;
  Status result = rpc_client_->SendRequest(server, req, res);
  
  if (!result.ok()) {
    LOG_DEBUG("AppendEntry to %s failed %s", server_.c_str(), result.ToString().c_str());
    return result;
  }
#if (LOG_LEVEL != LEVEL_NONE)
  google::protobuf::TextFormat::PrintToString(cmd_res, &text_format);
  LOG_DEBUG("AppendEntry Receive from %s, message :\n%s", server_.c_str(), text_format.c_str());
#endif

  if (cmd_res.aers().term() > raft_->current_term_) {
    raft_->StepDown(cmd_res.aers().term());
  } else {
    if (cmd_res.aers().status()) {
      last_agree_index_ = prev_log_index + num_entries;
      raft_->AdvanceCommitIndex();
      next_index_ = last_agree_index_ + 1;
    } else {
      if (next_index_ > 1) --next_index_;
    }
  }
  return Status::OK();
}

} // namespace floyd
