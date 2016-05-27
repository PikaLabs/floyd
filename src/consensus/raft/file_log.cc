#include <sys/types.h>
#include <dirent.h>
#include "floyd_util.h"
#include "file_log.h"

namespace floyd {
namespace raft {

const std::string kManifest = "manifest";
const std::string kLog = "log";

static std::string MakeFileName(const std::string &name, uint64_t number,
                                const char *suffix) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%010llu.%s",
           static_cast<unsigned long long>(number), suffix);
  return name + buf;
}

std::string LogFileName(const std::string &name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, kLog.c_str());
}

FileLog::FileLog(const std::string &path)
    : memory_log_(),
      // metadata_(),
      // current_sync_(new Sync(0)),
      path_(path),
      manifest_(NULL),
      table_(NULL) {

  Status s;

  if (path.back() != '/') path_ = path + '/';

  slash::CreateDir(path_);
  // std::vector<uint64_t> entryids = GetEntryIds();

  // std::string filename = path_ + kManifest;

  //	if (!ReadMetadata(filename, metadata_)) {
  //		metadata_.set_entries_start(1);
  //		metadata_.set_entries_end(0);
  //	}

  Recover();

  // memory_log_.TruncatePrefix(metadata_.entries_start());
  // memory_log_.TruncateSuffix(metadata_.entries_end());

  // TruncatePrefix(metadata_.entries_start());
  // TruncateSuffix(metadata_.entries_end());
  // for (uint64_t id = metadata_.entries_start(); id <=
  // metadata_.entries_end(); ++id) {
  //	path = format("%016lx", id);
  //	Log::Entry e = Read(path);
  //	std::vector<Log::Entry*> es;
  //	es.push_back(&e);
  //	memory_log_.Append(es);
  //}

  Log::metadata = manifest_->metadata_.raft_metadata();
}

FileLog::~FileLog() {
  if (manifest_ != NULL) {
    UpdateMetadata();
    delete manifest_;
  }
  if (table_ != NULL) {
    table_->Sync();
    delete table_;
  }
  //	if (current_sync_->fds.empty())
  //		current_sync_->completed = true;
}

bool FileLog::Recover() {
  slash::RandomRWFile *file;

  std::string filename = path_ + kManifest;
  if (!slash::FileExists(filename)) {

    slash::NewRandomRWFile(filename, &file);
    manifest_ = new Manifest(file);
    manifest_->Clear();

    filename = LogFileName(path_, ++manifest_->log_number_);
    if (!Table::Open(filename, &table_)) {
      fprintf(stderr, "[WARN] (%s:%d) open %s failed\n", __FILE__, __LINE__,
              filename.c_str());
    }

    current_sync_ = std::unique_ptr<Sync>(new Sync(0, manifest_, table_));

  } else {

    // mainifest recover
    slash::NewRandomRWFile(filename, &file);
    manifest_ = new Manifest(file);
    manifest_->Recover();

    // log recover
    std::vector<std::string> files;

    int ret = slash::GetChildren(path_, files);
    if (ret != 0) {
      fprintf(stderr, "recover failed when open path %s, %s", path_.c_str(),
              strerror(ret));
      return false;
    }

    sort(files.begin(), files.end());
    for (size_t i = 0; i < files.size(); i++) {
      // printf (" files[%lu]=%s klog=%s\n", i, files[i].c_str(), kLog.c_str());
      if (files[i].find(kLog) != std::string::npos) {
        int cnt = RecoverFromFile(path_ + files[i],
                                  manifest_->metadata_.entries_start(),
                                  manifest_->metadata_.entries_end());

        // TODO
        if (cnt > 0) {
        }
      }
    }

    if (table_ == NULL) {
      filename = LogFileName(path_, ++manifest_->log_number_);
      if (!Table::Open(filename, &table_)) {
        fprintf(stderr, "[WARN] (%s:%d) open %s failed\n", __FILE__, __LINE__,
                filename.c_str());
      }
    }

    current_sync_ =
        std::unique_ptr<Sync>(new Sync(GetLastLogIndex(), manifest_, table_));

    manifest_->Save();
  }
  return true;
}

bool Manifest::Recover() {
  Slice result;
  int nread = 0;

  Status s = file_->Read(nread, kIdLength + kOffsetLength, &result, scratch);
  if (!s.ok()) {
    return false;
  }

  const char *p = result.data();
  memcpy((char *)(&log_number_), p, kIdLength);
  memcpy((char *)(&length_), p + kIdLength, kOffsetLength);

  nread += kIdLength + kOffsetLength;

  s = file_->Read(nread, length_, &result, scratch);
  if (!s.ok()) {
    return false;
  }

  return metadata_.ParseFromArray(result.data(), result.size());
}

int FileLog::RecoverFromFile(const std::string &file,
                             const uint64_t entry_start,
                             const uint64_t entry_end) {

  if (table_ != NULL) {
    delete table_;
    table_ = NULL;
  }

  if (!Table::Open(file, &table_)) {
    return -1;
  }

  // stale table
  if (table_->header_->entry_start > entry_end ||
      table_->header_->entry_end < entry_start) {
    delete table_;
    table_ = NULL;
    slash::DeleteFile(file);
    return 0;
  }

  Iterator *iter = table_->NewIterator();
  iter->SeekToFirst();

  std::vector<Log::Entry *> es;

  for (; iter->Valid(); iter->Next()) {
    if (iter->msg.entry_id < entry_start || iter->msg.entry_id > entry_end)
      continue;

    // TODO memory leak
    Log::Entry *e = new Log::Entry;
    e->ParseFromArray(iter->msg.pb, iter->msg.length);
    // e.ParseFromString(iter->msg.pb);

    es.push_back(e);
  }
  memory_log_.Append(es);

  delete iter;

  return es.size();
}

std::pair<uint64_t, uint64_t> FileLog::Append(std::vector<Entry *> &entries) {
  // int fd = -1;
  std::string path;
  std::pair<uint64_t, uint64_t> range = memory_log_.Append(entries);
  for (uint64_t i = range.first; i <= range.second; ++i) {
    Log::Entry entry = memory_log_.GetEntry(i);

    Slice result;
    int nwrite = table_->AppendEntry(i, entry);

    // TODO
    if (nwrite > 0) {
    }
    // path = path_ + format("%016lx", i);
    // fd = ProtocolToFile(memory_log_.GetEntry(i), path);
    // if (fd != -1)
    //	current_sync_->fds.push_back({fd, true});
  }

  *(manifest_->metadata_.mutable_raft_metadata()) = Log::metadata;
  manifest_->Update(memory_log_.GetStartLogIndex(),
                    memory_log_.GetLastLogIndex());

  // metadata_.set_entries_start(memory_log_.GetStartLogIndex());
  // metadata_.set_entries_end(memory_log_.GetLastLogIndex());
  // path = path_ + kManifest;
  // fd = ProtocolToFile(metadata_, path);
  // if (fd != -1)
  //	current_sync_->fds.push_back({fd, true});
  // current_sync_->last_index = range.second;
  return range;
}

void FileLog::TruncateSuffix(uint64_t last_index) {
  uint64_t current_index = GetLastLogIndex();
  memory_log_.TruncateSuffix(last_index);
  UpdateMetadata();

  while (current_index > last_index) {
    // whole log file should be abondon
    if (table_->header_->entry_start >= last_index) {
      delete table_;

      std::string filename = LogFileName(path_, manifest_->log_number_);
      slash::DeleteFile(filename);

      if (manifest_->log_number_ == 1) {
        std::string filename = LogFileName(path_, manifest_->log_number_);
        Table::Open(filename, &table_);
        break;
      }

      filename = LogFileName(path_, --manifest_->log_number_);
      Table::Open(filename, &table_);

      current_index = table_->header_->entry_end;
    } else {
      Iterator *iter = table_->NewIterator();
      iter->SeekToLast();

      std::vector<Log::Entry *> es;

      for (; iter->Valid(); iter->Prev()) {
        current_index = iter->msg.entry_id;
        if (current_index <= last_index) {
          break;
        }
        iter->TruncateEntry();
      }
      delete iter;
    }
  }

  table_->Sync();
}

int Table::AppendEntry(uint64_t index, Log::Entry &entry) {
  int length = entry.ByteSize();
  int nwrite = kIdLength + 2 * kOffsetLength + length;
  int byte_size;

  Slice result;
  if (nwrite > 1024 * 4) {
    if (backing_store_ != NULL) {
      delete backing_store_;
    }
    backing_store_ = new char[nwrite];
    byte_size = Serialize(index, length, entry, &result, backing_store_);
  } else {
    byte_size = Serialize(index, length, entry, &result, scratch);
  }

  Status s = file_->Write(header_->filesize, result);
  if (!s.ok()) {
    return -1;
  }

  header_->filesize += byte_size;
  header_->entry_end = index;

  return byte_size;
}

int Table::Serialize(uint64_t index, int length, Log::Entry &entry,
                     Slice *result, char *scratch) {
  int offset = 0;
  memcpy(scratch, &index, sizeof(kIdLength));
  offset += kIdLength;
  memcpy(scratch + offset, &length, sizeof(kOffsetLength));
  offset += kOffsetLength;

  if (!entry.SerializeToArray(scratch + offset, length)) {
    return -1;
  }
  offset += length;
  memcpy(scratch + offset, &offset, sizeof(kOffsetLength));

  offset += kOffsetLength;

  *result = Slice(scratch, offset);
  return offset;
}

std::unique_ptr<Log::Sync> FileLog::TakeSync() {
  std::unique_ptr<Sync> other(new Sync(GetLastLogIndex(), manifest_, table_));
  std::swap(other, current_sync_);
  return std::move(other);
}

// TODO we need file size, maybe mmap isnot suitable;
// bool FileLog::ReadMetadata() {
//  char *p = manifest_->GetData();
//  if (p != NULL) {
//	  if (!metadata_.ParseFromArray(static_cast<const char*>(data),
//				static_cast<int>(read_size))) {
//  }
//	return true;
//}

void Manifest::Clear() {
  std::string ip("");
  int port = 0;
  floyd::raft::log::MetaData *tmpMeta = new floyd::raft::log::MetaData();
  tmpMeta->set_voted_for_ip(ip);
  tmpMeta->set_voted_for_port(port);
  tmpMeta->set_current_term(1);
  metadata_.set_allocated_raft_metadata(tmpMeta);
}

void Manifest::Update(uint64_t entry_start, uint64_t entry_end) {
  metadata_.set_entries_start(entry_start);
  metadata_.set_entries_end(entry_end);
  Save();
}

bool Manifest::Save() {
  length_ = metadata_.ByteSize();
  int nwrite = kIdLength + kOffsetLength + length_;
  int offset = 0;

  memcpy(scratch, &log_number_, sizeof(kIdLength));
  offset += kIdLength;
  memcpy(scratch + offset, &length_, sizeof(kOffsetLength));
  offset += kOffsetLength;

  if (!metadata_.SerializeToArray(scratch + offset, length_)) {
    return false;
  }

  Slice result(scratch, nwrite);

  Status s = file_->Write(0, result);
  if (!s.ok()) {
    return false;
  }

  file_->Sync();
  return true;
}

void FileLog::UpdateMetadata() {
  // metadata_.set_entries_start(memory_log_.GetStartLogIndex());
  // metadata_.set_entries_end(memory_log_.GetLastLogIndex());
  manifest_->Update(memory_log_.GetStartLogIndex(),
                    memory_log_.GetLastLogIndex());
}

// Log::Entry FileLog::Read(std::string& path) {
//	Log::Entry entry;
//	FileToProtocol(entry, path);
//	return entry;
//}

uint64_t FileLog::GetStartLogIndex() { return memory_log_.GetStartLogIndex(); }

uint64_t FileLog::GetLastLogIndex() { return memory_log_.GetLastLogIndex(); }

uint64_t FileLog::GetSizeBytes() { return memory_log_.GetSizeBytes(); }

FileLog::Entry &FileLog::GetEntry(uint64_t index) {
  return memory_log_.GetEntry(index);
}

FileLog::Sync::Sync(uint64_t lastindex, Manifest *mn, Table *tb)
    : Log::Sync(lastindex), manifest(mn), table(tb) {}

void FileLog::Sync::Wait() {
  // TODO sync
  if (manifest != NULL) {
    manifest->Save();
  }
  if (table != NULL) {
    table->Sync();
  }
  //	for (auto it = fds.begin(); it != fds.end(); ++it) {
  //		fsync(it->first);
  //		if (it->second)
  //			close(it->first);
  //	}
}

//
// Table related
//
bool Table::Open(const std::string &filename, Table **table) {
  *table = NULL;

  bool is_exist = slash::FileExists(filename);

  slash::RandomRWFile *file;
  slash::Status s = slash::NewRandomRWFile(filename, &file);
  if (!s.ok()) {
    fprintf(stderr, "[WARN] (%s:%d) open %s failed, %s\n", __FILE__, __LINE__,
            filename.c_str(), s.ToString().c_str());
    return false;
  }

  // ReadHeader
  Header *header = new Header;
  if (is_exist && !ReadHeader(file, header)) {
    return false;
  }

  *table = new Table(file, header);

  return true;
}

// bool Table::Open(slash::RandomRWFile* file, Table** table) {
//  *table = NULL;
//
//  // ReadHeader
//  Header *header = new Header;
//  if (!ReadHeader(file, header)) {
//    return false;
//  }
//
//  *table = new Table(file, header);
//
//  return true;
//}

bool Table::ReadHeader(slash::RandomRWFile *file, Header *h) {
  char scratch[256];
  Slice result;

  Status s = file->Read(0, kTableHeaderLength, &result, scratch);
  if (!s.ok()) {
    return false;
  }

  const char *p = result.data();
  memcpy((char *)(&(h->entry_start)), p, sizeof(uint64_t));
  memcpy((char *)(&(h->entry_end)), p + sizeof(uint64_t), sizeof(uint64_t));
  memcpy((char *)(&(h->filesize)), p + 2 * sizeof(uint64_t), sizeof(uint32_t));
  return true;
}

int Table::ReadMessage(int offset, Message *msg, bool from_end) {
  int nread = 0;
  int msg_offset;
  Slice result;
  Status s;

  if (from_end) {
    s = file_->Read(offset - kOffsetLength, kOffsetLength, &result, scratch);
    if (!s.ok()) {
      return -1;
    }

    msg_offset = *((int32_t *)(result.data()));
    offset -= msg_offset + kOffsetLength;
  }

  s = file_->Read(offset, kIdLength + kOffsetLength, &result, scratch);
  if (!s.ok()) {
    return -1;
  }

  nread += kIdLength + kOffsetLength;
  const char *p = result.data();
  memcpy((char *)(&(msg->entry_id)), p, kIdLength);
  memcpy((char *)(&(msg->length)), p + kIdLength, kOffsetLength);

  if (msg->length + kOffsetLength > 1024 * 4) {

    if (backing_store_ != NULL) {
      delete backing_store_;
    }
    backing_store_ = new char[msg->length + kOffsetLength];

    s = file_->Read(offset + nread, msg->length + kOffsetLength, &result,
                    backing_store_);
  } else {
    s = file_->Read(offset + nread, msg->length + kOffsetLength, &result,
                    scratch);
  }

  if (!s.ok()) {
    return -1;
  }

  nread += msg->length + kOffsetLength;

  p = result.data();
  msg->pb = p;
  memcpy((char *)(&(msg->begin_offset)), p + msg->length, kOffsetLength);

  assert(static_cast<int>(msg->begin_offset + kOffsetLength) == nread);

  return nread;
}

bool Table::Sync() {
  Slice result((char *)header_, sizeof(Header));

  Status s = file_->Write(0, result);
  if (!s.ok()) {
    return false;
  }

  if (file_ != NULL) {
    file_->Sync();
  }
  return true;
}

Iterator *Table::NewIterator() {
  Iterator *iter = new Iterator(this);
  return iter;
}

//
// discarded
//
/*

int FileLog::ProtocolToFile(google::protobuf::Message& in,
                                                                         std::string&
path) {
        int fd = open(path.c_str(), O_CREAT|O_WRONLY|O_TRUNC, 0644);
        if (fd == -1)
                return -1;

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

int FileLog::FileToProtocol(google::protobuf::Message& out,
                                                                         std::string&
path) {
        int fd = open(path.c_str(), O_RDONLY, 0644);
        if (fd == -1)
                return -1;

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
void FileLog::TruncatePrefix(uint64_t first_index) {
        uint64_t old = GetStartLogIndex();
        memory_log_.TruncatePrefix(first_index);
        UpdateMetadata();
        for (uint64_t index = old; index < GetStartLogIndex(); ++index)
                unlink((path_ + format("%016lx", index)).c_str());
}


bool FileLog::ReadMetadata(std::string& path, floyd::raft::filelog::MetaData&
metadata) {
        int ret = FileToProtocol(metadata, path);
        if (ret == -1)
                return false;
        return true;
}

std::vector<uint64_t> FileLog::GetEntryIds() {
        DIR* dir;
        struct dirent* ptr;
        std::vector<uint64_t> entry_ids;
        if ((dir = opendir(path_.c_str())) == NULL)
                return entry_ids;
        while ((ptr = readdir(dir)) != NULL) {
                if (strcmp(ptr->d_name, ".") == 0 ||
                                strcmp(ptr->d_name, "..") == 0 ||
                                strcmp(ptr->d_name, "metadata") == 0)
                        continue;
                uint64_t entry_id;
                int matched = sscanf(ptr->d_name, "%016lx", &entry_id);
                if (matched != 1)
                        continue;
                entry_ids.push_back(entry_id);
        }
        closedir(dir);
        return entry_ids;
}
*/
}
}
