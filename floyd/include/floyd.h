#ifndef FLOYD_H_
#define FLOYD_H_

#include <string>

#include "floyd/include/floyd_options.h"
#include "slash/include/slash_status.h"

namespace floyd {

using slash::Status;

class Floyd {
 public:
  static Status Open(const Options& options, Floyd** floyd);

  Floyd() { }
  virtual ~Floyd();

  // TODO(anan) maybe remove 
  virtual Status Start() = 0;

  virtual Status Write(const std::string& key, const std::string& value) = 0;
  virtual Status DirtyWrite(const std::string& key, const std::string& value) = 0;
  virtual Status Delete(const std::string& key) = 0;
  virtual Status Read(const std::string& key, std::string& value) = 0;
  virtual Status DirtyRead(const std::string& key, std::string& value) = 0;
  //Status ReadAll(std::map<std::string, std::string>& kvMap);
  //Status DirtyReadAll(std::map<std::string, std::string>& kvMap);
  //Status TryLock(const std::string& key);
  //Status UnLock(const std::string& key);

  // return true if leader has been elected
  virtual bool GetLeader(std::string& ip_port) = 0;
  virtual bool GetServerStatus(std::string& msg) = 0;
  
  // log level can be modified
  virtual void set_log_level(const int log_level) = 0;

 private:
  // No coping allowed
  Floyd(const Floyd&);
  void operator=(const Floyd&);
};

} // namespace floyd
#endif
