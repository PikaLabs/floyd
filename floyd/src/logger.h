#ifndef LOGGER_H_
#define LOGGER_H_
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <atomic>
#include <stdarg.h>
#include <sys/time.h>
#include <sys/syscall.h>

// Use class Logger to write to a file
// Usage:
//    1) first create a logger;
//      Logger* logger;
//      NewLogger(path, &logger);
//    2) use LOGV(LEVEL, logger, fmt, ...);
//      eg: LOGV(DEBUG_LEVEL, logger, "%s", "example message");
namespace floyd {

class Logger;

// LOGV
int NewLogger(const std::string& fname, Logger** result);
// void LogFlush(const Logger* info_log);


//
// Internal use
//
#define _FILE strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__

#define LOG_FMT "%s:%d] "

#define LOGV(level, info_log, message, args...) \
  Logv(level, info_log, LOG_FMT message, _FILE, __LINE__, ##args)

//enum {
//  DEBUG_LEVEL = 0x01,
//  INFO_LEVEL  = 0x02,
//  WARN_LEVEL  = 0x03,
//  ERROR_LEVEL = 0x04,
//  FATAL_LEVEL = 0x05,
//  NONE_LEVEL  = 0x06
//};

//#define DEBUG_LEVEL   0x01
//#define INFO_LEVEL    0x02
//#define WARN_LEVEL    0x03
//#define ERROR_LEVEL   0x04
//#define FATAL_LEVEL   0x05
//#define NONE_LEVEL    0x06

void Logv(const int log_level, Logger* info_log,
         const char* format, ...);

extern const char* LEVEL_TAG;
#define gettid() syscall(SYS_gettid)
static inline pid_t pid();


static inline pid_t pid() {
  return gettid();
}

class Logger {
 private:
  FILE* file_;
  std::atomic_size_t log_size_;
  const static uint64_t flush_every_seconds_ = 5;
  std::atomic_uint_fast64_t last_flush_micros_;
  bool flush_pending_;
  int log_level_;

 public:
  Logger(FILE* f);
  ~Logger();

  void Flush();
  void Logv(int log_level, const char* format, va_list ap);
  size_t GetLogFileSize() const { return log_size_; }

  void set_log_level(const int log_level) {
    log_level_ = log_level;
  }
  int log_level() const {
    return log_level_;
  }
};

} // namespace floyd
#endif
