#ifndef FLOYD_UTIL_H_
#define FLOYD_UTIL_H_

#include <string>
#include <vector>
#include <leveldb/db.h>

#include "csapp.h"

#include "slash_status.h"

#include "status.h"

namespace floyd {

// Parse leveldb::Status to slash::Status
slash::Status ParseStatus(leveldb::Status& status);
// Parse pink::Status to slash::Status
slash::Status ParseStatus(pink::Status& status);

const std::string kMetaPrefix = "#";

std::string SerializeKey(const std::string key);
std::string ParseKey(const std::string encode_key);

std::string SerializeUser(const std::string &ip, const int port);
void ParseUser(const std::string &value, std::string *ip, int *port);

std::string SerializeUserMeta(const std::string &user);
std::string SerializeUserData(const std::string &user, const std::string &key);
void ParseUserData(const std::string &value, std::string *user,
                   std::string *key);
}

#endif
