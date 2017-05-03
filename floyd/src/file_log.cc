#include "floyd/src/file_log.h"

#include <iostream>
#include <sys/types.h>
#include <dirent.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <google/protobuf/text_format.h>

#include "floyd/src/logger.h"
//#include "slash/include/slash_mutex.h"

namespace floyd {

const std::string kManifest = "manifest";
const std::string kLog = "floyd.log";

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
  : path_(path),
    manifest_(NULL),
    last_table_(NULL),
    cache_size_(30000) {
  if (path.back() != '/') {
    path_ = path + '/';
  }

  slash::CreateDir(path_);
  Recover();
}

FileLog::~FileLog() {
  if (manifest_ != NULL) {
    manifest_->Save();
    delete manifest_;
  }
  for (auto& it : tables_) {
    delete it.second;
  }
}

bool FileLog::Recover() {
  slash::RandomRWFile *file;

  std::string filename = path_ + kManifest;
  if (!slash::FileExists(filename)) {
    LOG_DEBUG("FileLog::Recover newly node");
    slash::NewRandomRWFile(filename, &file);
    manifest_ = new Manifest(file);
  } else {
    // manifest recover
    slash::NewRandomRWFile(filename, &file);
    manifest_ = new Manifest(file);
    manifest_->Recover();

    // log recover
    std::vector<std::string> files;

    int ret = slash::GetChildren(path_, files);
    if (ret != 0) {
      LOG_ERROR("recover failed when open path %s, %s", path_.c_str(),
              strerror(ret));
      return false;
    }

    sort(files.begin(), files.end());
    for (size_t i = 0; i < files.size(); i++) {
      // printf (" files[%lu]=%s klog=%s\n", i, files[i].c_str(), kLog.c_str());
      if (files[i].find(kLog) != std::string::npos) {
        Table* tmp;
        if (!GetTable(path_ + files[i], &tmp)) {
          fprintf(stderr, "[WARN] (%s:%d) open %s failed\n", __FILE__, __LINE__,
                  files[i].c_str());
        }
        LOG_DEBUG("FileLog::Recover old node with exist file %s", (path_ + files[i]).c_str());
        last_table_ = tmp;
      }
    }
  }

  if (last_table_ == NULL) {
    filename = LogFileName(path_, ++manifest_->meta_.file_num);
    if (!GetTable(filename, &last_table_)) {
      fprintf(stderr, "[WARN] (%s:%d) open %s failed\n", __FILE__, __LINE__,
              filename.c_str());
    }
    LOG_DEBUG("FileLog::Recover new last_table_ %s", filename.c_str());
    manifest_->Save();
    last_table_->Sync();
  }
#if (LOG_LEVEL != LEVEL_NONE)
  manifest_->Dump();
#endif
  return true;
}

bool FileLog::GetTable(const std::string &file, Table** table) {
  // mu_.AssertLock();
  *table = NULL;
  auto tmp = tables_.find(file);
  Table *tb;

  if (tmp == tables_.end()) {
    if (!Table::Open(file, &tb)) {
      return false;
    }
    tables_[file] = tb;

    // limit cache size to cache_size
    if (tables_.size() > cache_size_) {
      Table *smallest_table = tables_.begin()->second;
      delete smallest_table;
      tables_.erase(tables_.begin());
    }
  } else {
    tb = tmp->second;
  }

  // stale table
  if (tb->header_->entry_end > 0          // not new table;
      && (tb->header_->entry_start > manifest_->meta_.entry_end
          || tb->header_->entry_end < manifest_->meta_.entry_start)) {
    LOG_DEBUG("FileLog::GetTable stale file %s with entry(%lu, %lu), global entry(%lu, %lu)",
              file.c_str(), tb->header_->entry_start, tb->header_->entry_end,
              manifest_->meta_.entry_start, manifest_->meta_.entry_end);
    delete tb;
    slash::DeleteFile(file);
    tables_.erase(file);
    return false;
  }

  *table = tb;
  return true;
}

std::pair<uint64_t, uint64_t> FileLog::Append(std::vector<Entry *> &entries) {
  slash::MutexLock l(&mu_);
  uint64_t start = manifest_->meta_.entry_end + 1;
  uint64_t end = start + entries.size() - 1;
  for (uint64_t i = start; i <= end; ++i) {
    int nwrite = last_table_->AppendEntry(i, *(entries[i - start]));
    SplitIfNeeded();
  }

  manifest_->meta_.entry_end = end;
  manifest_->Save();
  return {start, end}; 
}

void FileLog::UpdateMetadata(uint64_t current_term, std::string voted_for_ip, 
                             int32_t voted_for_port) {
  uint32_t ip = 0;
  if (!voted_for_ip.empty()) {
    struct in_addr in;
    inet_aton(voted_for_ip.c_str(), &in); 
    ip = in.s_addr;
  }

  slash::MutexLock l(&mu_);
  manifest_->meta_.current_term = current_term;
  manifest_->meta_.voted_for_ip = ip;
  manifest_->meta_.voted_for_port = voted_for_port;
  manifest_->Save();
}

uint64_t FileLog::GetStartLogIndex() {
  slash::MutexLock l(&mu_);
  return manifest_->meta_.entry_start;
}

uint64_t FileLog::GetLastLogIndex() {
  slash::MutexLock l(&mu_);
  return manifest_->meta_.entry_end;
}

bool FileLog::GetLastLogTermAndIndex(uint64_t* last_log_term, uint64_t* last_log_index) {
  slash::MutexLock l(&mu_);
  *last_log_index = manifest_->meta_.entry_end;
  if (*last_log_index == 0) {
    *last_log_term = 0;
  } else {
    Entry entry;
    last_table_->GetEntry(*last_log_index, &entry);
    *last_log_term = entry.term();
  }
  return true;
}

bool FileLog::GetEntry(uint64_t index, Entry* entry) {
  slash::MutexLock l(&mu_);
  Table* tmp = NULL;
  if (index >= last_table_->header_->entry_start && index <= last_table_->header_->entry_end) {
    tmp = last_table_;
  } else {
    for (auto it = tables_.rbegin(); it != tables_.rend(); it++) {
      if (index >= it->second->header_->entry_start && index <= it->second->header_->entry_end) {
        tmp = last_table_;
        break;
      }
    }
  }

  if (tmp != NULL) {
    return tmp->GetEntry(index, entry);
  }
  
  return false;
}

uint64_t FileLog::current_term() {
  slash::MutexLock l(&mu_);
  return manifest_->meta_.current_term;
}

std::string FileLog::voted_for_ip() {
  struct in_addr in;
  {
    slash::MutexLock l(&mu_);
    in.s_addr = manifest_->meta_.voted_for_ip;
  }
  std::string ip = inet_ntoa(in);
  return ip; 
}

uint32_t FileLog::voted_for_port() {
  slash::MutexLock l(&mu_);
  return manifest_->meta_.voted_for_port;
}

void FileLog::SplitIfNeeded() {
  if (last_table_->header_->filesize > 16 * 1024 * 1024) {
    Table * tmp;
    std::string filename = LogFileName(path_, ++manifest_->meta_.file_num);
    if (!GetTable(filename, &tmp)) {
      fprintf(stderr, "[WARN] (%s:%d) open %s failed\n", __FILE__, __LINE__,
              filename.c_str());
    }
    
    uint64_t next = last_table_->header_->entry_end + 1;
    LOG_DEBUG("FileLog::SplitIfNeeded create new file %s with entry_start (%lu)",
              filename.c_str(), next);
    last_table_ = tmp;
    last_table_->header_->entry_start = next;
    last_table_->header_->entry_end = next;
  }
}

bool FileLog::TruncateSuffix(uint64_t last_index) {
  slash::MutexLock l(&mu_);
  uint64_t current_index = manifest_->meta_.entry_end;

  while (current_index > last_index) {
    // whole log file should be abondon
    if (last_table_->header_->entry_start >= last_index) {
      if (!TruncateLastTable()) {
        return false;
      }
      current_index = last_table_->header_->entry_end;
    } else {
      Iterator *iter = last_table_->NewIterator();
      iter->SeekToLast();

      std::vector<Entry *> es;

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

  last_table_->Sync();
  return true;
}

bool FileLog::TruncateLastTable() {
  //mu_.AssertLock();

  std::string filename = LogFileName(path_, manifest_->meta_.file_num);
  tables_.erase(tables_.find(filename));
  delete last_table_;
  last_table_ = NULL;
  slash::DeleteFile(filename);

  if (manifest_->meta_.file_num > 1) {
    --manifest_->meta_.file_num;
  }

  filename = LogFileName(path_, manifest_->meta_.file_num);
  Table* tmp;
  if (!GetTable(filename, &tmp)) {
    return false;
  }
  last_table_ = tmp;
  return true;
}

//
// Table
//
int Table::AppendEntry(uint64_t index, Entry &entry) {
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

//  std::cout << "AppendEntry, id: " << index << std::endl << std::flush;

  LOG_DEBUG("Table::AppendEntry index=%lu, length=%d, before file_size=%d, byte_size=%d",
            index, length, header_->filesize, byte_size);
  Status s = file_->Write(header_->filesize, result);
  if (!s.ok()) {
    return -1;
  }

  header_->filesize += byte_size;
  header_->entry_end = index;
  result = Slice((char *)header_, sizeof(Header));
  s = file_->Write(0, result);
  if (!s.ok()) {
    return -1;
  }

  LOG_DEBUG("Table::AppendEntry header_  filesize=%d, entry_start=%lu, entry_end=%lu",
            header_->filesize, header_->entry_start, header_->entry_end);

  return byte_size;
}

bool Table::GetEntry(uint64_t index, Entry* entry) {
  if (index < header_->entry_start || index > header_->entry_end) {
    return false;
  }

  Iterator *iter = NewIterator();
  iter->SeekToLast();

  std::vector<Entry *> es;
  for (; iter->Valid(); iter->Prev()) {
    if (iter->msg.entry_id == index) {
      break;
    }
  }
  bool ret = entry->ParseFromArray(iter->msg.pb, iter->msg.length);
  delete iter;
  return ret;
}

int Table::Serialize(uint64_t index, int length, Entry &entry,
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

// TODO we need file size, maybe mmap isnot suitable;
// bool FileLog::ReadMetadata() {
//  char *p = manifest_->GetData();
//  if (p != NULL) {
//	  if (!metadata_.ParseFromArray(static_cast<const char*>(data),
//				static_cast<int>(read_size))) {
//  }
//	return true;
//}

//
// Mainifest
//
bool Manifest::Recover() {
  Slice result;
  Status s = file_->Read(0, sizeof(Meta), &result, scratch);
  if (!s.ok()) {
    LOG_DEBUG("Manifest::Recover file Write failed, %s", s.ToString().c_str());
    return false;
  }

  const char *p = result.data();
  memcpy((char *)(&meta_), p, sizeof(Meta));
  return true;
}

bool Manifest::Save() {
  Slice buf((char *)&meta_, sizeof(Meta));
  Status s = file_->Write(0, buf);
  if (!s.ok()) {
    LOG_DEBUG("Manifest::Save file Write failed, %s", s.ToString().c_str());
    return false;
  }

#if (LOG_LEVEL != LEVEL_NONE)
  LOG_DEBUG("Manifest::Save after save Manifest", s.ToString().c_str());
  Dump();
#endif

  file_->Sync();
  return true;
}

void Manifest::Dump() {
  LOG_INFO ("          file_num  :  %lu\n"
          "       entry_start  :  %lu\n"
          "         entry_end  :  %lu\n"
          "      current_term  :  %lu\n"
          "      voted_for_ip  :  %u\n"
          "    voted_for_port  :  %u\n",
          meta_.file_num, meta_.entry_start, meta_.entry_end,
          meta_.current_term, meta_.voted_for_ip, meta_.voted_for_port);
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
  memcpy((char *)(&(h->filesize)), p + 2 * sizeof(uint64_t), sizeof(uint64_t));
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
// TODO anan rm
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

} // namespace floyd
