#ifndef FLOYD_FILE_LOG_H_
#define FLOYD_FILE_LOG_H_

#include <string>
#include <map>
#include "floyd.pb.h"

#include "slash/include/env.h"
#include "slash/include/slash_slice.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

using slash::Status;
using slash::Slice;

namespace floyd {

class Iterator;
class Manifest;
class Table;

// TODO(anan) 
//    1. we don't remove log files
class FileLog {
 public:
  explicit FileLog(const std::string& path);
  ~FileLog();

  std::pair<uint64_t, uint64_t> Append(std::vector<Entry*>& entries);
  //void TruncatePrefix(uint64_t first_index) { first_index = 0; assert(false); }
  bool TruncateSuffix(uint64_t last_index);

  bool GetEntry(uint64_t index, Entry* entry);
  uint64_t GetStartLogIndex();
  uint64_t GetLastLogIndex();
  bool GetLastLogTermAndIndex(uint64_t* last_log_term, uint64_t* last_log_index);

  void UpdateMetadata(uint64_t current_term, std::string voted_for_ip,
                      int32_t voted_for_port);

  uint64_t current_term();
  std::string voted_for_ip();
  uint32_t voted_for_port(); 

  void DumpSingleFile(const std::string &filename);

  Manifest *manifest_;

 private:
  std::string path_;
  Table *last_table_;
  int cache_size_;

  slash::Mutex mu_;
  std::map<std::string, Table*> tables_;

  bool Recover();
  bool GetTable(const std::string &file, Table** table);
  void SplitIfNeeded();

  bool TruncateLastTable();

  // No copying allowed
  FileLog(const FileLog&);
  void operator=(const FileLog&);
};

const size_t kIdLength = sizeof(uint64_t);
const size_t kOffsetLength = sizeof(uint64_t);
const size_t kTableHeaderLength = 2 * kIdLength + kOffsetLength;
//const size_t kManifestMetaLength = 4 * kIdLength + 2 * sizeof(uint32_t);

//
// Manifest structure:
//  | length(int32)  | FileLogMetaData pb message(length) |
// 
class Manifest {
 public:
  struct Meta {
    // FileLog needed
    uint64_t file_num;
    uint64_t entry_start;
    uint64_t entry_end;

    // Raft needed
    uint64_t current_term;
    uint32_t voted_for_ip;
    uint32_t voted_for_port;
    uint64_t apply_index;
    
    Meta()
      : file_num(0LL), entry_start(1LL), entry_end(0LL), 
        current_term(1), voted_for_ip(0), voted_for_port(0),
        apply_index(0LL) { }
  };

  explicit Manifest(slash::RandomRWFile* file)
      : file_(file) { }

  bool Recover();
  void Update(uint64_t entry_start, uint64_t entry_end);
  bool Save();
  
  void Dump();

  slash::RandomRWFile *file_;
  Meta meta_;

 private:
  char scratch[256];

  // No copying allowed
  Manifest(const Manifest&);
  void operator=(const Manifest&);
};

//
// Table structure:
//    Header :  | entry_start(uint64)  |  entry_end(uint64)  | EOF offset(int32) |
//    Body   :  | Entry i |  Entry i+1 | ... |
// Entry structure:
//    | entry_id(uint64) | length(int32) | pb format msg(length bytes) | begin_offset(int32) |
//
class Table {
 public:
  struct Header {
    uint64_t entry_start;
    uint64_t entry_end;
    uint64_t filesize;

    Header() : entry_start(1), entry_end(0), filesize(kTableHeaderLength) {}
  };

  struct Message {
    uint64_t entry_id;
    int32_t length;
    const char *pb;
    int32_t begin_offset;
  };

  //static bool Open(slash::RandomRWFile* file, Table** table);
  static bool Open(const std::string &filename, Table** table);
  static bool ReadHeader(slash::RandomRWFile* file, Header *header);

  int ReadMessage(int offset, Message *msg, bool from_end = false);
  int AppendEntry(uint64_t index, Entry& entry);

  void TruncateEntry(uint64_t index, int offset) {
    header_->entry_end = index - 1;
    header_->filesize = offset;
  }

  bool GetEntry(uint64_t index, Entry* entry);
  bool Sync();
  void DumpHeader();

  Iterator* NewIterator();
  ~Table() {
    if (file_ != NULL) {
      //file_->Sync();
      Sync();
    }
    delete file_;

    if (backing_store_ != NULL)
      delete backing_store_;
    delete header_;
  }

  //slash::RandomRWFile *file() { return file_; }

  // TODO maybe need atomic
  Header *header_;
  slash::RandomRWFile *file_;

 private:
  Table(slash::RandomRWFile* file, Header *header)
      : header_(header),
        file_(file),
        backing_store_(NULL) {}

  int Serialize(uint64_t index, int length, Entry &entry, Slice *result, char *scratch);


  char scratch[1024 * 4];
  char *backing_store_;

  // No copying allowed
  Table(const Table&);
  void operator=(const Table&);
};

// Single Table Iterator
class Iterator {
 public:
  Iterator(Table *table)
      : table_(table),
      file_(table_->file_),
      //header_(header),
      offset_(0), valid_(false) {} 

  ~Iterator() {}

  bool Valid() {
    return valid_;
  }
  void SeekToFirst() {
    offset_ = kTableHeaderLength;
    valid_ =  offset_ < table_->header_->filesize ? true : false;
    Next();
  }
  
  void SeekToLast() {
    offset_ = table_->header_->filesize;
    valid_ =  offset_ > kTableHeaderLength ? true : false;
    Prev();
  }

  void Next() {
    if (!valid_ || offset_ >= table_->header_->filesize) {
      valid_ = false;
      return;
    }

    int nread = table_->ReadMessage(offset_, &msg);
    if (nread <= 0) {
      valid_ = false;
    } else {
      offset_ += nread;
    }
  }

  void Prev() {
    if (!valid_ || offset_ - kOffsetLength <= kTableHeaderLength) {
      valid_ = false;
      return;
    }

    int nread = table_->ReadMessage(offset_, &msg, true);
    if (nread <= 0) {
      valid_ = false;
    } else {
      offset_ -= nread;
    }
  }

  void TruncateEntry() {
    table_->TruncateEntry(msg.entry_id, offset_);
  }

  Table::Message msg;

private:

  Table *table_;
  slash::RandomRWFile *file_;
  //Table::Header* header_;
  uint64_t offset_;
  bool valid_;

  // No copying allowed
  Iterator(const Iterator&);
  void operator=(const Iterator&);
};

} // namespace floyd
#endif
