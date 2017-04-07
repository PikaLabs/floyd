#include "memory_log.h"

namespace floyd {
namespace raft {

MemoryLog::MemoryLog()
    : start_index_(1), entries_(), current_sync_(new Sync(0)) {}

MemoryLog::~MemoryLog() { current_sync_->completed = true; }

std::pair<uint64_t, uint64_t> MemoryLog::Append(
    std::vector<Entry*>& new_entries) {
  uint64_t first_index = start_index_ + entries_.size();
  uint64_t last_index = first_index + new_entries.size() - 1;
  for (auto it = new_entries.begin(); it != new_entries.end(); ++it)
    entries_.push_back(**it);
  current_sync_->last_index = last_index;
  return {first_index, last_index};
}

Log::Entry& MemoryLog::GetEntry(uint64_t index) {
  uint64_t offset = index - start_index_;
  return entries_.at(offset);
}

uint64_t MemoryLog::GetStartLogIndex() { return start_index_; }

uint64_t MemoryLog::GetLastLogIndex() {
  return start_index_ + entries_.size() - 1;
}

uint64_t MemoryLog::GetSizeBytes() {
  uint64_t size = 0;
  for (auto it = entries_.begin(); it < entries_.end(); ++it)
    size += it->ByteSize();
  return size;
}

std::unique_ptr<Log::Sync> MemoryLog::TakeSync() {
  std::unique_ptr<Sync> other(new Sync(GetLastLogIndex()));
  std::swap(other, current_sync_);
  return other;
}

void MemoryLog::TruncatePrefix(uint64_t first_index) {
  if (first_index > start_index_) {
    entries_.erase(entries_.begin(),
                   entries_.begin() +
                       std::min(first_index - start_index_, entries_.size()));
    start_index_ = first_index;
  }
}

void MemoryLog::TruncateSuffix(uint64_t last_index) {
  if (last_index < start_index_)
    entries_.clear();
  else if (last_index < start_index_ - 1 + entries_.size())
    entries_.resize(last_index - start_index_ + 1);
}

void MemoryLog::UpdateMetadata() {}
}
}
