#ifndef __FLOYD_CONSENSUS_FILE_LOG_H__
#define __FLOYD_CONSENSUS_FILE_LOG_H__

#include "filelog_meta.pb.h"
#include "memory_log.h"

#include "env.h"
#include "slash_slice.h"
#include "slash_status.h"
#include "slash_mutex.h"


using slash::Status;
using slash::Slice;

namespace floyd {
namespace raft {

class Iterator;
class Manifest;
class Table;

class FileLog : public Log {
  class Sync : public Log::Sync {
   public:
    Sync(uint64_t lastindex, Manifest* mn, Table* tb);
    void Wait();

    //std::vector<std::pair<int, bool>> fds;
    Manifest* manifest;
    Table* table;
  };
 public:
  typedef floyd::raft::Entry Entry;

  explicit FileLog(const std::string& path);
  ~FileLog();
  std::pair<uint64_t, uint64_t> Append(std::vector<Entry*>& entries);
  std::unique_ptr<Log::Sync> TakeSync();
  void TruncatePrefix(uint64_t first_index) { first_index = 0; assert(false); }
  void TruncateSuffix(uint64_t last_index);

  Entry& GetEntry(uint64_t index);
  uint64_t GetStartLogIndex();
  uint64_t GetLastLogIndex();
  uint64_t GetSizeBytes();
  void UpdateMetadata();

 protected:
  MemoryLog memory_log_;
  //floyd::raft::filelog::MetaData metadata_;
  std::unique_ptr<Sync> current_sync_;
  std::string path_;

  Manifest *manifest_;
  Table *table_;
  //uint64_t log_number_;

  //bool ReadMetadata(std::string& filename,
  //                  floyd::raft::Filelog::MetaData& metadata);

  //std::vector<uint64_t> GetEntryIds();
  Entry Read(std::string& path);
  int ProtocolToFile(google::protobuf::Message& in, std::string& path);
  int FileToProtocol(google::protobuf::Message& out, std::string& path);

 private:

  bool ReadMetadata(std::string& path, floyd::raft::filelog::MetaData& metadata);
  bool Recover();
  int RecoverFromFile(const std::string &file, const uint64_t entry_start, const uint64_t entry_end);
};


const size_t kIdLength = sizeof(uint64_t);
const size_t kOffsetLength = sizeof(uint32_t);
const size_t kTableHeaderLength = 2 * kIdLength + kOffsetLength;

//
// Manifest structure:
//  | log_number(uint64) | length(int32)  | pb message(length) |
// 
class Manifest {
 public:
  explicit Manifest(slash::RandomRWFile* file)
      : file_(file),
      log_number_(0LL),
      length_(0) {}

  bool Recover();
  void Update(uint64_t entry_start, uint64_t entry_end);
  void Clear();
  bool Save();


  slash::RandomRWFile *file_;
  uint64_t log_number_;
  int length_;

  floyd::raft::filelog::MetaData metadata_;

 private:

  int Serialize(uint64_t index, int length, Slice *result, char *scratch);


  char scratch[1024];

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
    uint32_t filesize;

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
  int AppendEntry(uint64_t index, Log::Entry &entry);

  void TruncateEntry(uint64_t index, int offset) {
    header_->entry_end = index - 1;
    header_->filesize = offset;
  }

  bool Sync();

  Iterator* NewIterator();
  ~Table() {
    if (file_ != NULL) {
      file_->Sync();
    }
    delete file_;

    if (backing_store_ != NULL)
      delete backing_store_;
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

  int Serialize(uint64_t index, int length, Log::Entry &entry, Slice *result, char *scratch);


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
  uint32_t offset_;
  bool valid_;

  // No copying allowed
  Iterator(const Iterator&);
  void operator=(const Iterator&);
};

} // namespace raft
} // namespace floyd
#endif
