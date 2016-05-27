#ifndef __FLOYD_DB_H__
#define __FLOYD_DB_H__

#include <iostream>
#include <string>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include "status.h"

namespace floyd {

class DbBackend {
 public:
  DbBackend() {}

  virtual ~DbBackend() {}

  virtual Status Open() = 0;

  virtual Status Get(const std::string& key, std::string& value) = 0;
  virtual Status GetAll(std::map<std::string, std::string>& kvMap) = 0;

  virtual Status Set(const std::string& key, const std::string& value) = 0;

  virtual Status Delete(const std::string& key) = 0;

  virtual int LockIsAvailable(std::string& user, std::string& key) = 0;

  virtual Status LockKey(const std::string& user, const std::string& key) = 0;

  virtual Status UnLockKey(const std::string& user, const std::string& key) = 0;

  virtual Status DeleteUserLocks(const std::string& user) = 0;
};

class LeveldbBackend : public DbBackend {
 public:
  LeveldbBackend(std::string path) { dbPath = path; }

  virtual ~LeveldbBackend() { delete db; }

  Status Open();

  Status Get(const std::string& key, std::string& value);

  Status GetAll(std::map<std::string, std::string>& kvMap);

  Status Set(const std::string& key, const std::string& value);

  Status Delete(const std::string& key);

  virtual int LockIsAvailable(std::string& user, std::string& key);

  virtual Status LockKey(const std::string& user, const std::string& key);

  virtual Status UnLockKey(const std::string& user, const std::string& key);

  virtual Status DeleteUserLocks(const std::string& user);

 private:
  leveldb::DB* db;
  std::string dbPath;
};

}  // namespace floyd
#endif
