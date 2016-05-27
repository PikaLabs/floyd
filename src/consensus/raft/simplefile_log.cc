#include <sys/types.h>
#include <dirent.h>
#include "floyd_util.h"
#include "simplefile_log.h"

namespace floyd {
namespace raft {

SimpleFileLog::SimpleFileLog(const std::string& parent_dir)
    : memory_log_(), metadata_(), current_sync_(new Sync(0)) {
  // handle parent dir
  if (parent_dir.at(parent_dir.length() - 1) != '/')
    parent_dir_ = parent_dir + '/';
  else
    parent_dir_ = parent_dir;
  mkdir(parent_dir_.c_str(), 0755);

  std::vector<uint64_t> entryids = GetEntryIds();
  std::string path = parent_dir_ + "metadata";
  int ret = ReadMetadata(path, metadata_);
  if (!ret) {
    metadata_.set_entries_start(1);
    metadata_.set_entries_end(0);
  }

  memory_log_.TruncatePrefix(metadata_.entries_start());
  memory_log_.TruncateSuffix(metadata_.entries_end());
  TruncatePrefix(metadata_.entries_start());
  TruncateSuffix(metadata_.entries_end());
  for (uint64_t id = metadata_.entries_start(); id <= metadata_.entries_end();
       ++id) {
    path = format("%016lx", id);
    Log::Entry e = Read(path);
    std::vector<Log::Entry*> es;
    es.push_back(&e);
    memory_log_.Append(es);
  }

  Log::metadata = metadata_.raft_metadata();
}

SimpleFileLog::~SimpleFileLog() {
  if (current_sync_->fds.empty()) current_sync_->completed = true;
}

int SimpleFileLog::ProtocolToFile(google::protobuf::Message& in,
                                  std::string& path) {
  int fd = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd == -1) return -1;

  uint64_t len = in.ByteSize();
  char* data = new char[len + 1];
  if (!in.SerializeToArray(data, len)) {
    close(fd);
    return -1;
  }

  ssize_t written = write(fd, data, len);
  if (written == -1) {
    delete data;
    close(fd);
    return -1;
  }

  delete data;
  return fd;
}

int SimpleFileLog::FileToProtocol(google::protobuf::Message& out,
                                  std::string& path) {
  int fd = open(path.c_str(), O_RDONLY, 0644);
  if (fd == -1) return -1;

  struct stat st;
  uint64_t file_size = 0;
  if (stat(path.c_str(), &st) == -1) {
    close(fd);
    return -1;
  }
  file_size = st.st_size;

  char* data = new char[file_size + 1];
  ssize_t read_size = read(fd, data, file_size);
  if (read_size == -1) {
    delete data;
    close(fd);
    return -1;
  }

  if (!out.ParseFromArray(static_cast<const char*>(data),
                          static_cast<int>(read_size))) {
    delete data;
    close(fd);
    return -1;
  }

  close(fd);
  return 0;
}

std::pair<uint64_t, uint64_t> SimpleFileLog::Append(
    std::vector<Entry*>& entries) {
  int fd = -1;
  std::string path;
  std::pair<uint64_t, uint64_t> range = memory_log_.Append(entries);
  for (uint64_t i = range.first; i <= range.second; ++i) {
    path = parent_dir_ + format("%016lx", i);
    fd = ProtocolToFile(memory_log_.GetEntry(i), path);
    if (fd != -1) current_sync_->fds.push_back({fd, true});
  }

  *metadata_.mutable_raft_metadata() = Log::metadata;
  metadata_.set_entries_start(memory_log_.GetStartLogIndex());
  metadata_.set_entries_end(memory_log_.GetLastLogIndex());
  path = parent_dir_ + "metadata";
  fd = ProtocolToFile(metadata_, path);
  if (fd != -1) current_sync_->fds.push_back({fd, true});
  current_sync_->last_index = range.second;
  return range;
}

std::unique_ptr<Log::Sync> SimpleFileLog::TakeSync() {
  std::unique_ptr<Sync> other(new Sync(GetLastLogIndex()));
  std::swap(other, current_sync_);
  return std::move(other);
}

void SimpleFileLog::TruncatePrefix(uint64_t first_index) {
  uint64_t old = GetStartLogIndex();
  memory_log_.TruncatePrefix(first_index);
  UpdateMetadata();
  for (uint64_t index = old; index < GetStartLogIndex(); ++index)
    unlink((parent_dir_ + format("%016lx", index)).c_str());
}

void SimpleFileLog::TruncateSuffix(uint64_t last_index) {
  uint64_t old = GetLastLogIndex();
  memory_log_.TruncateSuffix(last_index);
  UpdateMetadata();
  for (uint64_t index = old; index > GetLastLogIndex(); --index)
    unlink((parent_dir_ + format("%016lx", index)).c_str());
}

bool SimpleFileLog::ReadMetadata(
    std::string& path, floyd::raft::simplefilelog::MetaData& metadata) {
  int ret = FileToProtocol(metadata, path);
  if (ret == -1) return false;
  return true;
}

// bug: reset tmpMeta
void SimpleFileLog::UpdateMetadata() {
  std::string ip("");
  int port = 0;
  floyd::raft::log::MetaData* tmpMeta = new floyd::raft::log::MetaData();
  tmpMeta->set_voted_for_ip(ip);
  tmpMeta->set_voted_for_port(port);
  tmpMeta->set_current_term(1);
  metadata_.set_allocated_raft_metadata(tmpMeta);
  metadata_.set_entries_start(memory_log_.GetStartLogIndex());
  metadata_.set_entries_end(memory_log_.GetLastLogIndex());
  std::string path = parent_dir_ + "metadata";
  int fd = ProtocolToFile(metadata_, path);
  if (fd != -1) fsync(fd);

  // TODO close
}

std::vector<uint64_t> SimpleFileLog::GetEntryIds() {
  DIR* dir;
  struct dirent* ptr;
  std::vector<uint64_t> entry_ids;
  if ((dir = opendir(parent_dir_.c_str())) == NULL) return entry_ids;
  while ((ptr = readdir(dir)) != NULL) {
    if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0 ||
        strcmp(ptr->d_name, "metadata") == 0)
      continue;
    uint64_t entry_id;
    int matched = sscanf(ptr->d_name, "%016lx", &entry_id);
    if (matched != 1) continue;
    entry_ids.push_back(entry_id);
  }
  closedir(dir);
  return entry_ids;
}

Log::Entry SimpleFileLog::Read(std::string& path) {
  Log::Entry entry;
  FileToProtocol(entry, path);
  return entry;
}

uint64_t SimpleFileLog::GetStartLogIndex() {
  return memory_log_.GetStartLogIndex();
}

uint64_t SimpleFileLog::GetLastLogIndex() {
  return memory_log_.GetLastLogIndex();
}

uint64_t SimpleFileLog::GetSizeBytes() { return memory_log_.GetSizeBytes(); }

SimpleFileLog::Entry& SimpleFileLog::GetEntry(uint64_t index) {
  return memory_log_.GetEntry(index);
}

SimpleFileLog::Sync::Sync(uint64_t lastindex) : Log::Sync(lastindex), fds() {}

void SimpleFileLog::Sync::Wait() {
  for (auto it = fds.begin(); it != fds.end(); ++it) {
    fsync(it->first);
    if (it->second) close(it->first);
  }
}
}
}
