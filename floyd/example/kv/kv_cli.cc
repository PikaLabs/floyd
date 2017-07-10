#include "kv_cli.h"

#include <algorithm>
#include <google/protobuf/text_format.h>

#include "logger.h"
#include "sdk.pb.h"

#include "slash/include/slash_status.h"


namespace floyd {

void Tokenize(const std::string& str, std::vector<std::string>& tokens, const char& delimiter = ' ') {
  size_t prev_pos = str.find_first_not_of(delimiter, 0);
  size_t pos = str.find(delimiter, prev_pos);

  while (prev_pos != std::string::npos || pos != std::string::npos) {
    std::string token(str.substr(prev_pos, pos - prev_pos));
    //printf ("find a token(%s), prev_pos=%u pos=%u\n", token.c_str(), prev_pos, pos);
    tokens.push_back(token);

    prev_pos = str.find_first_not_of(delimiter, pos);
    pos = str.find_first_of(delimiter, prev_pos);
  }
}

///// Server //////
Server::Server(const std::string& str) {
  size_t pos = str.find(':');
  ip = str.substr(0, pos);
  port = atoi(str.substr(pos + 1).c_str());
}

///// Option //////
Option::Option(const std::string& _server)
  : timeout(1000),
  server(_server) {
}

Option::Option(const Option& option)
  : timeout(option.timeout),
    server(option.server) {
}


////// Cluster //////
Cluster::Cluster(const Option& option)
  : option_(option) {
  pb_cli_ = pink::NewPbCli();
  Init();
}

bool Cluster::Init() {
  Status result = pb_cli_->Connect(option_.server.ip, option_.server.port);
  if (!result.ok()) {
    LOG_ERROR("cluster connect error, %s", result.ToString().c_str());
    return false;
  }
  return true;
}

slash::Status Cluster::Write(const std::string& key, const std::string& value) {
  Request request;
  request.set_type(Type::WRITE);

  Request_Write* write_req = request.mutable_write();
  write_req->set_key(key);
  write_req->set_value(value);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }
  Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

  //LOG_INFO("Write OK, status is %d, msg is %s\n", response.write().status(), response.write().msg().c_str());
  if (response.code() == StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::IOError("Write failed with status " + response.msg());
  }
}

slash::Status Cluster::Read(const std::string& key, std::string* value) {
  Request request;
  request.set_type(Type::READ);

  Request_Read* read_req = request.mutable_read();
  read_req->set_key(key);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }

  Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

//  std::string text_format;
//  google::protobuf::TextFormat::PrintToString(response, &text_format);
//  LOG_DEBUG("Read Recv message :\n%s", text_format.c_str());

//  LOG_INFO("Read OK, status is %d, value is %s\n", response.read().status(), response.read().value().c_str());
  if (response.code() == StatusCode::kOk) {
    *value = response.read().value();
    return Status::OK();
  } else if (response.code() == StatusCode::kNotFound) {
    return Status::IOError("Read not found ");
  } else {
    return Status::IOError("Read failed with status " + response.msg());
  }
}

slash::Status Cluster::DirtyRead(const std::string& key, std::string* value) {
  Request request;
  request.set_type(Type::DIRTYREAD);

  Request_Read* read_req = request.mutable_read();
  read_req->set_key(key);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }

  Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

//  std::string text_format;
//  google::protobuf::TextFormat::PrintToString(response, &text_format);
//  LOG_DEBUG("DirtyRead Recv message :\n%s", text_format.c_str());

  *value = response.read().value();

//  LOG_INFO("DirtyRead OK, status is %d, value is %s\n", response.read().status(), response.read().value().c_str());
  if (response.code() == StatusCode::kOk) {
    *value = response.read().value();
    return Status::OK();
  } else if (response.code() == StatusCode::kNotFound) {
    return Status::IOError("DirtyRead not found ");
  } else {
    return Status::IOError("DirtyRead failed with status " + response.msg());
  }
}

slash::Status Cluster::GetStatus(std::string* msg) {
  Request request;
  request.set_type(Type::STATUS);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }


  Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

  *msg = response.server_status().msg();

  //LOG_INFO("Status OK, msg:\n%s", response.server_status().msg().c_str());
  return Status::OK();
}

slash::Status Cluster::DirtyWrite(const std::string& key, const std::string& value) {
  Request request;
  request.set_type(Type::DIRTYWRITE);

  Request_Write* write_req = request.mutable_write();
  write_req->set_key(key);
  write_req->set_value(value);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }
  Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

//  std::string text_format;
//  google::protobuf::TextFormat::PrintToString(response, &text_format);
//  LOG_DEBUG("DirtyWrite Recv message :\n%s", text_format.c_str());

//  LOG_INFO("DirtyWrite OK, status is %d, msg is %s\n", response.write().status(), response.write().msg().c_str());
  if (response.code() == StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::IOError("DirtyWrite failed with status " + response.msg());
  }
}

slash::Status Cluster::Delete(const std::string& key) {
  Request request;
  request.set_type(Type::DELETE);

  Request_Delete* del_req = request.mutable_del();
  del_req->set_key(key);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }
  Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

//  std::string text_format;
//  google::protobuf::TextFormat::PrintToString(response, &text_format);
//  LOG_DEBUG("Delete Recv message :\n%s", text_format.c_str());

//  LOG_INFO("Delete OK, status is %d, msg is %s\n", response.del().status(), response.del().msg().c_str());
  if (response.code() == StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::IOError("Del failed with status " + response.msg());
  }
}

slash::Status Cluster::set_log_level(const int log_level) {
  Request request;
  request.set_type(Type::LOGLEVEL);
  request.set_log_level(log_level);

  if (!pb_cli_->Available()) {
    if (!Init()) {
      return Status::IOError("init failed");
    }
  }
  Status result = pb_cli_->Send(&request);
  if (!result.ok()) {
    LOG_ERROR("Send error: %s", result.ToString().c_str());
    return Status::IOError("Send failed, " + result.ToString());
  }

  Response response;
  result = pb_cli_->Recv(&response);
  if (!result.ok()) {
    LOG_ERROR("Recv error: %s", result.ToString().c_str());
    return Status::IOError("Recv failed, " + result.ToString());
  }

//  std::string text_format;
//  google::protobuf::TextFormat::PrintToString(response, &text_format);
//  LOG_DEBUG("LOGLEVEL Recv message :\n%s", text_format.c_str());
  return Status::OK();
}

} // namspace floyd
