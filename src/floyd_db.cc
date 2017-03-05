#include "floyd_db.h"

#include "floyd_util.h"

namespace floyd {

Status LeveldbBackend::Open() {
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status result = leveldb::DB::Open(options, dbPath, &db);
  if (result.ok()) {
    return Status::OK();
  } else {
    return Status::Corruption(result.ToString());
  }
}

Status LeveldbBackend::Get(const std::string& key, std::string& value) {
  leveldb::Status result = db->Get(leveldb::ReadOptions(), key, &value);
  if (result.ok()) {
    return Status::OK();
  } else {
    return Status::Corruption(result.ToString());
  }
}

Status LeveldbBackend::GetAll(std::map<std::string, std::string>& kvMap) {
  // std::string key = "ha";
  // std::string value;
  // leveldb::Status result = db->Get(leveldb::ReadOptions(), key, &value);
  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    leveldb::Slice key = it->key();
    if (key[0] == kMetaPrefix[0]) {
      continue;
    }
    kvMap.insert(std::make_pair(it->key().ToString(), it->value().ToString()));
  }

  delete it;
  return Status::OK();
  ;
}

Status LeveldbBackend::Set(const std::string& key, const std::string& value) {
  leveldb::Status result = db->Put(leveldb::WriteOptions(), key, value);
  if (result.ok()) {
    return Status::OK();
  } else {
    return Status::Corruption(result.ToString());
  }
}

Status LeveldbBackend::Delete(const std::string& key) {
  leveldb::Status result = db->Delete(leveldb::WriteOptions(), key);
  if (result.ok()) {
    return Status::OK();
  } else {
    return Status::Corruption(result.ToString());
  }
}

int LeveldbBackend::LockIsAvailable(std::string& user, std::string& key) {
  int is_available = -1;
  std::string lock_key = SerializeKey(key);
  std::string value;
  leveldb::Status ret = db->Get(leveldb::ReadOptions(), lock_key, &value);

  // already locked
  if (ret.ok()) {
    // session check
    is_available = 0;

    if (user == value) {
    }

  } else if (ret.IsNotFound()) {
    is_available = 1;
  } else {
  }

  return is_available;
}

// LockIsAvailable should ok;
Status LeveldbBackend::LockKey(const std::string& user,
                               const std::string& key) {
  std::string value;

  // std::string user_meta = SerializeUserMeta(user);
  // leveldb::Status result = db->Get(leveldb::ReadOptions(), user_meta,
  // &value);
  // int key_num = 0;
  // if (result.ok()) {
  //   key_num = *((int32_t *)value.data());
  // } else if (!result.IsNotFound()) {
  //   return result;
  // }
  // key_num++;

  std::string lock_key = SerializeKey(key);
  std::string user_data = SerializeUserData(user, key);

  leveldb::WriteBatch batch;
  batch.Put(lock_key, user);
  // batch.Put(user_meta, std::string((char *)&key_num, sizeof(key_num)));
  batch.Put(user_data, "");

  leveldb::Status result = db->Write(leveldb::WriteOptions(), &batch);
  if (result.ok()) {
    return Status::OK();
  } else {
    return Status::Corruption(result.ToString());
  }
}

Status LeveldbBackend::UnLockKey(const std::string& user,
                                 const std::string& key) {
  leveldb::WriteBatch batch;
  std::string value;
  std::string lock_key = SerializeKey(key);

  // leveldb::Status result = db->Get(leveldb::ReadOptions(), lock_key, &value);

  // if (result.ok()) {
  std::string user_data = SerializeUserData(user, key);
  leveldb::Status result = db->Get(leveldb::ReadOptions(), user_data, &value);
  if (result.ok()) {
    batch.Delete(user_data);

    //   std::string user_meta = SerializeUserMeta(user);
    //   result = db->Get(leveldb::ReadOptions(), user_meta, value);
    //   int key_num = 0;
    //   if (result.ok()) {
    //     key_num = *((int32_t *)value.data());
    //     if (--key_num > 0) {
    //       batch.Set(user_meta, std::string((char *)&key_num,
    // sizeof(key_num)));
    //     } else {
    //       batch.Delete(user_meta);
    //     }
    //   }
  }
  batch.Delete(lock_key);
  result = db->Write(leveldb::WriteOptions(), &batch);
  //} else {
  //  // non exist key
  //}

  if (result.ok()) {
    return Status::OK();
  } else {
    return Status::Corruption(result.ToString());
  }
}

Status LeveldbBackend::DeleteUserLocks(const std::string& user) {
  leveldb::WriteBatch batch;
  std::string value;
  std::string user_data_prefix = SerializeUserData(user, "");

  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

  it->Seek(user_data_prefix);
  for (; it->Valid(); it->Next()) {
    std::string user_key = it->key().ToString();

    if (user_key.substr(0, user_data_prefix.size()) == user_data_prefix) {
      std::string parse_user, parse_key;
      ParseUserData(it->key().ToString(), &parse_user, &parse_key);
      batch.Delete(it->key());
      batch.Delete(parse_key);
    }
  }

  leveldb::Status result = db->Write(leveldb::WriteOptions(), &batch);
  if (result.ok()) {
    return Status::OK();
  } else {
    return Status::Corruption(result.ToString());
  }
}

}  // namespace floyd
