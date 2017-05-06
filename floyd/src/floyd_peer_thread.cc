#include "floyd/src/floyd_peer_thread.h"

#include <climits>
#include <google/protobuf/text_format.h>
#include "floyd/src/floyd_primary_thread.h"
#include "floyd/src/floyd_context.h"
#include "floyd/src/floyd_client_pool.h"
#include "floyd/src/file_log.h"
#include "floyd/src/floyd.pb.h"
#include "floyd/src/logger.h"

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"

namespace floyd {

Peer::Peer(std::string server, FloydContext* context, FloydPrimary* primary,
  FileLog* log, ClientPool* pool)
  : server_(server),
    context_(context),
    primary_(primary),
    log_(log),
    pool_(pool),
    next_index_(1) {
}

int Peer::StartThread() {
  bg_thread_.set_thread_name("FloydPr" + server_.substr(server_.find(':')));
  return bg_thread_.StartThread();
}

Peer::~Peer() {
  LOG_INFO("Peer(%s) exit!!!", server_.c_str());
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
  LOG_DEBUG("Peer(%s)::DoRequestVote", peer->server_.c_str());
  Status result = peer->RequestVote();
  if (!result.ok()) {
    LOG_ERROR("Peer(%s) failed to RequestVote caz %s.", peer->server_.c_str(), result.ToString().c_str());
  }
}

Status Peer::RequestVote() {
  if (context_->role() != Role::kCandidate) {
    return Status::OK();
  }
  
  // TODO (anan) log->getEntry() need lock
  uint64_t last_log_term;
  uint64_t last_log_index;
  context_->log()->GetLastLogTermAndIndex(&last_log_term, &last_log_index);
  uint64_t current_term = context_->current_term();

  CmdRequest req;
  req.set_type(Type::RequestVote);
  CmdRequest_RequestVote* request_vote = req.mutable_request_vote();
  request_vote->set_ip(context_->local_ip());
  request_vote->set_port(context_->local_port());
  request_vote->set_term(current_term);
  request_vote->set_last_log_term(last_log_term);
  request_vote->set_last_log_index(last_log_index);

#if (LOG_LEVEL != LEVEL_NONE)
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(req, &text_format);
  LOG_DEBUG("Send RequestVote to %s, message :\n%s", server_.c_str(), text_format.c_str());
#endif

  CmdResponse res;
  Status result = pool_->SendAndRecv(server_, req, &res);
  
  if (!result.ok()) {
    LOG_DEBUG("RequestVote to %s failed %s", server_.c_str(), result.ToString().c_str());
    return result;
  }

#if (LOG_LEVEL != LEVEL_NONE)
  google::protobuf::TextFormat::PrintToString(res, &text_format);
  LOG_DEBUG("Recv RequestVote from %s, message :\n%s", server_.c_str(), text_format.c_str());
#endif

  uint64_t res_term = res.request_vote().term();
  if (result.ok() && context_->role() == Role::kCandidate) {
    if (res.code() == StatusCode::kOk) {    // granted
      LOG_DEBUG("Peer(%s)::RequestVote granted will Vote and check", server_.c_str());
      if (context_->VoteAndCheck(res_term)) {
        primary_->AddTask(kBecomeLeader);  
      }
    } else {
      LOG_DEBUG("Vote request denied by %s, res_term=%lu, current_term=%lu",
                server_.c_str(), res_term, current_term);
      // TODO wangkang-xy should be follower by the response
    }
  }

  return result;
}

void Peer::AddHeartBeatTask() {
  AddAppendEntriesTask();
}

void Peer::AddAppendEntriesTask() {
  bg_thread_.Schedule(DoAppendEntries, this);
}

void Peer::DoAppendEntries(void *arg) {
  Peer* peer = static_cast<Peer*>(arg);
  LOG_DEBUG("Peer(%s) DoAppendEntries", peer->server_.c_str());
  Status result = peer->AppendEntries();
  if (!result.ok()) {
    LOG_ERROR("Peer(%s) failed to AppendEntries caz %s.", peer->server_.c_str(), result.ToString().c_str());
  }
}

uint64_t Peer::GetMatchIndex() {
  slash::MutexLock l(&mu_);
  return (next_index_ - 1);
}

Status Peer::AppendEntries() {
  uint64_t last_log_index = log_->GetLastLogIndex();
  uint64_t prev_log_index = next_index_ - 1;
  if (prev_log_index > last_log_index) {
    return Status::InvalidArgument("prev_Log_index > last_log_index");
  }

  uint64_t prev_log_term = 0;
  if (prev_log_index != 0) {
    Entry entry;
    log_->GetEntry(prev_log_index, &entry);
    prev_log_term = entry.term();
  }

  CmdRequest req;
  CmdRequest_AppendEntries* append_entries = req.mutable_append_entries();
  req.set_type(Type::AppendEntries);
  append_entries->set_ip(context_->local_ip());
  append_entries->set_port(context_->local_port());
  append_entries->set_term(context_->current_term());
  append_entries->set_prev_log_index(prev_log_index);
  append_entries->set_prev_log_term(prev_log_term);

  uint64_t num_entries = 0;
  for (uint64_t index = next_index_; index <= last_log_index; ++index) {
    Entry entry;
    log_->GetEntry(index, &entry);
    *append_entries->add_entries() = entry;
    uint64_t request_size = append_entries->ByteSize();
    if (request_size < context_->append_entries_size_once() ||
        num_entries == 0)
      ++num_entries;
    else
      append_entries->mutable_entries()->RemoveLast();
  }
  append_entries->set_commit_index(
      std::min(context_->commit_index(), prev_log_index + num_entries));

#if (LOG_LEVEL != LEVEL_NONE)
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(req, &text_format);
  LOG_DEBUG("AppendEntry Send to %s, message :\n%s", server_.c_str(), text_format.c_str());
#endif

  CmdResponse res;
  Status result = pool_->SendAndRecv(server_, req, &res);

  if (!result.ok()) {
    LOG_DEBUG("AppendEntry to %s failed %s", server_.c_str(), result.ToString().c_str());
    return result;
  }
#if (LOG_LEVEL != LEVEL_NONE)
  google::protobuf::TextFormat::PrintToString(res, &text_format);
  LOG_DEBUG("AppendEntry Receive from %s, message :\n%s", server_.c_str(), text_format.c_str());
#endif

  // TODO wangkang-xy should be follower by the response
  //uint64_t res_term = res.append_entries().term();
  //if (result.ok() && res_term > context_->current_term()) {
  //  //TODO(anan) maybe combine these 2 steps
  //  context_->BecomeFollower(res_term);
  //  primary_->ResetElectLeaderTimer();
  //}
  
  if (result.ok() && context_->role() == Role::kLeader) {
    if (res.code() == StatusCode::kOk) {
      next_index_ = prev_log_index + num_entries + 1;
      primary_->AddTask(kAdvanceCommitIndex);
    } else {
      if (next_index_ > 1) --next_index_;
    }
  }
  return result;
}

} // namespace floyd
