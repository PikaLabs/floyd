#include "floyd/src/floyd_context.h"
#include "floyd/src/logger.h"

namespace floyd {

FloydContext::FloydContext(const floyd::Options& opt,
    Log* log)
  : options_(opt),
  log_(log),
  current_term_(0),
  role_(Role::kFollower),
  voted_for_port_(0),
  leader_port_(0),
  vote_quorum_(0),
  commit_index_(0),
  apply_cond_(&apply_mu_) {
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&stat_rw_, &attr);
  }

FloydContext::~FloydContext() {
  pthread_rwlock_destroy(&stat_rw_);
}

void FloydContext::RecoverInit() {
  assert(log_ != NULL);
  slash::RWLock(&stat_rw_, true);
  if (log_->metadata.has_current_term())
    current_term_ = log_->metadata.current_term();
  if (log_->metadata.has_voted_for_ip() &&
      log_->metadata.has_voted_for_port()) {
    voted_for_ip_ = log_->metadata.voted_for_ip();
    voted_for_port_ = log_->metadata.voted_for_port();
  }
  role_ = Role::kFollower;
}

void FloydContext::BecomeFollower(uint64_t new_term,
      const std::string leader_ip, int leader_port) {
  assert(current_term_ <= new_term);
  slash::RWLock(&stat_rw_, true);
  if (current_term_ < new_term) {
    current_term_ = new_term;
    voted_for_ip_ = "";
    voted_for_port_ = 0;
    LogApply();
  }
  if (!leader_ip.empty() && leader_port != 0) {
    leader_ip_ = leader_ip;
    leader_port_ = leader_port;
  }
  role_ = Role::kFollower;

  LOG_DEBUG("BecomeFollower: with current_term_(%lu) and new_term(%lu)",
      current_term_, new_term);
}

void FloydContext::BecomeCandidate() {
  assert(role_ == Role::kFollower || role_ == Role::kCandidate);
  slash::RWLock(&stat_rw_, true);
  switch(role_) {
    case Role::kFollower:
      LOG_DEBUG("Become Candidate since prev leader timeout, prev term: %lu, prev leader is (%s:%d)",
          current_term_, leader_ip_.c_str(), leader_port_);
      break; 
    case Role::kCandidate:
      LOG_DEBUG("Become Candidate since prev election timeout, prev term: %lu",
          current_term_, leader_ip_.c_str(), leader_port_);
      break; 
    default:
      LOG_DEBUG("Become Candidate, should not be here, role: %d", role_);
  }

  ++current_term_;
  role_ = Role::kCandidate;
  leader_ip_.clear();
  leader_port_ = 0;
  voted_for_ip_ = options_.local_ip;
  voted_for_port_ = options_.local_port;
  vote_quorum_ = 1;
  LogApply();
}

void FloydContext::BecomeLeader() {
  assert(role_ == Role::kCandidate);
  slash::RWLock(&stat_rw_, true);
  role_ = Role::kLeader;
  leader_ip_ = options_.local_ip;
  leader_port_ = options_.local_port;
  LOG_DEBUG ("FloydContext::BecomeLeader I am become Leader!!");

  //ForEach(&PeerThread::BeginLeaderShip);
  // printf ("I am become Leader\n");

  // Append noop entry to guarantee that new leader can
  // Get commitindex timely.
  //std::vector<Log::Entry*> entries;
  //Log::Entry entry;
  //entry.set_type(floyd::raft::Entry::NOOP);
  //entry.set_term(current_term_);
  //entries.push_back(&entry);
  //Append(entries);

  //state_changed_.SignalAll();
}

bool FloydContext::AdvanceCommitIndex(uint64_t new_commit_index) {
  if (new_commit_index == 0) {
    return false;
  }
  slash::MutexLock l(&commit_mu_);
  if (commit_index_ >= new_commit_index) {
    // TODO why
    //if (commit_index > apply_index) {
    //  apply_->ScheduleApply();
    //}
    return false;
  }

  uint64_t last_log_index = log_->GetLastLogIndex();
  new_commit_index = std::min(last_log_index, new_commit_index);
  if (log_->GetEntry(new_commit_index).term() == current_term_) {
    commit_index_ = new_commit_index;
    LOG_DEBUG("FloydContext::AdvanceCommitIndex: commit_index=%ld", new_commit_index);
    return true;
  }
  return false;
}

void FloydContext::LogApply() {
  log_->metadata.set_current_term(current_term_);
  log_->metadata.set_voted_for_ip(voted_for_ip_);
  log_->metadata.set_voted_for_port(voted_for_port_);
  log_->UpdateMetadata();
}

bool FloydContext::VoteAndCheck(uint64_t vote_term) {
  slash::RWLock(&stat_rw_, true);
  LOG_DEBUG("FloydContext::VoteAndCheck: current_term=%lu vote_term=%lu vote_quorum_=%lu",
           current_term_, vote_term, vote_quorum_);
  if (current_term_ != vote_term) {
    return false;
  }
  return (++vote_quorum_) > (options_.members.size() / 2);
}

Status FloydContext::WaitApply(uint64_t commit_index, uint32_t timeout) { 
  while (commit_index_ < commit_index) {
    if (!apply_cond_.TimedWait(timeout)) {
      return Status::Timeout("apply timeout");
    }
  }
  return Status::OK();
}

// Peer ask my vote with it's ip, port, log_term and log_index
bool FloydContext::RequestVote(uint64_t term, const std::string ip,
    uint32_t port, uint64_t log_index, uint64_t log_term,
    uint64_t *my_term) {
  slash::RWLock l(&stat_rw_, true);
  if (term < current_term_) {
    return false; // stale term
  }

  if (term == current_term_) {
    if (!voted_for_ip_.empty()
        && (voted_for_ip_ != ip || voted_for_port_ != port)) {
      return false; // I have vote someone else
    }
  }
  
  uint64_t my_log_index = log_->GetLastLogIndex();
  uint64_t my_log_term = 0;
  if (my_log_index != 0) {
    my_log_term = log_->GetEntry(my_log_index).term();
  }
  if (log_term < my_log_term
      || (log_term == my_log_term && log_index < my_log_index)) {
    return false; // log index is not up-to-date as mine
  }

  // Got my vote
  voted_for_ip_ = ip;
  voted_for_port_ = port;
  *my_term = current_term_;
  LogApply();
  return true;
}

bool FloydContext::AppendEntries(uint64_t term,
    uint64_t pre_log_term, uint64_t pre_log_index,
    std::vector<Log::Entry*>& entries, uint64_t* my_term) {
  slash::RWLock l(&stat_rw_, true);
  // Check last log
  uint64_t my_log_index = log_->GetLastLogIndex();
  uint64_t my_log_term = 0;
  if (my_log_index != 0) {
    my_log_term = log_->GetEntry(my_log_index).term();
  }
  if (pre_log_index > my_log_index
      || pre_log_term != my_log_term) {
    return false;
  }

  // Append entry
  if (pre_log_index < log_->GetLastLogIndex()) {
      //log_->Resize(pre_log_index);
      log_->TruncateSuffix(pre_log_index + 1);
  }
  log_->Append(entries);
  *my_term = current_term_;

  return true;
}

}  // namespace floyd
