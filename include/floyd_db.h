#ifndef FLOYD_DB_H_
#define FLOYD_DB_H_

#include <iostream>
#include <string>
#include <map>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include "slash_status.h"

using slash::Status;

namespace floyd {

class LeveldbBackend {
 public:
  LeveldbBackend(std::string path) { path_ = path; }

  ~LeveldbBackend() { delete db_; }

  Status Open();

  Status Get(const std::string& key, std::string& value);

  Status GetAll(std::map<std::string, std::string>& kvMap);

  Status Set(const std::string& key, const std::string& value);

  Status Delete(const std::string& key);

  int LockIsAvailable(std::string& user, std::string& key);

  Status LockKey(const std::string& user, const std::string& key);

  Status UnLockKey(const std::string& user, const std::string& key);

  Status DeleteUserLocks(const std::string& user);

 private:
  leveldb::DB* db_;
  std::string path_;
};

}  // namespace floyd
#endif
