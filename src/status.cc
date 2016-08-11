// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include <iostream>
#include "status.h"

namespace floyd {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  memcpy(result, state, size + 5);
  return result;
}
void Status::init_by_code(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != kOk);
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 5];
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state_ = result;
}

Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  init_by_code(code, msg, msg2);
}

Status::Status(leveldb::Status& leveldb_status) {
  Code code = kOk;
  Slice status_slice = Slice(leveldb_status.ToString());
  if (status_slice.starts_with(Slice("OK"))) {
    code = kOk;
    state_ = NULL;
    return;
  } else if (status_slice.starts_with(Slice("NotFound"))) {
    code = kNotFound;
    status_slice.remove_prefix(sizeof("NotFound:"));
  } else if (status_slice.starts_with(Slice("TimeOut:"))) {
    code = kTimeOut;
    status_slice.remove_prefix(sizeof("TimeOut:"));
  } else if (status_slice.starts_with(Slice("Corruption:"))) {
    code = kCorruption;
    status_slice.remove_prefix(sizeof("Corruption:"));
  } else if (status_slice.starts_with(Slice("Not implemented:"))) {
    code = kNotSupported;
    status_slice.remove_prefix(sizeof("Not implemented:"));
  } else if (status_slice.starts_with(Slice("Invalid argument:"))) {
    code = kInvalidArgument;
    status_slice.remove_prefix(sizeof("Invalid argument:"));
  } else if (status_slice.starts_with(Slice("IO error:"))) {
    code = kIOError;
    status_slice.remove_prefix(sizeof("IO error:"));
  } else if (status_slice.starts_with(Slice("End file:"))) {
    code = kEndFile;
    status_slice.remove_prefix(sizeof("End file:"));
  } else {
    code = kCorruption;
  }
  init_by_code(code, status_slice, Slice(""));
}

Status::Status(pink::Status& pink_status) {
  Code code = kOk;
  Slice status_slice = Slice(pink_status.ToString());
  if (status_slice.starts_with(Slice("OK"))) {
    code = kOk;
    state_ = NULL;
    return;
  } else if (status_slice.starts_with(Slice("NotFound"))) {
    code = kNotFound;
    status_slice.remove_prefix(sizeof("NotFound:"));
  } else if (status_slice.starts_with(Slice("TimeOut:"))) {
    code = kTimeOut;
    status_slice.remove_prefix(sizeof("TimeOut:"));
  } else if (status_slice.starts_with(Slice("Corruption:"))) {
    code = kCorruption;
    status_slice.remove_prefix(sizeof("Corruption:"));
  } else if (status_slice.starts_with(Slice("Not implemented:"))) {
    code = kNotSupported;
    status_slice.remove_prefix(sizeof("Not implemented:"));
  } else if (status_slice.starts_with(Slice("Invalid argument:"))) {
    code = kInvalidArgument;
    status_slice.remove_prefix(sizeof("Invalid argument:"));
  } else if (status_slice.starts_with(Slice("IO error:"))) {
    code = kIOError;
    status_slice.remove_prefix(sizeof("IO error:"));
  } else if (status_slice.starts_with(Slice("End file:"))) {
    code = kEndFile;
    status_slice.remove_prefix(sizeof("End file:"));
  } else {
    code = kCorruption;
  }
  init_by_code(code, status_slice, Slice(""));
}

std::string Status::ToString() const {
  if (state_ == NULL) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kTimeOut:
        type = "TimeOut: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      case kEndFile:
        type = "End file: ";
        break;
      case kNetworkError:
        type = "Network error: ";
        break;
      default:
        snprintf(tmp, sizeof(tmp), "Unknown code(%d): ",
                 static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type);
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    result.append(state_ + 5, length);
    return result;
  }
}
}
