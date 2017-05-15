#ifndef LOGGER_H_
#define LOGGER_H_
#include <stdio.h>
#include <string>
#include <unistd.h>
//#include <fcntl.h>
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

int NewLogger(const std::string& fname, Logger** result);
//void LogFlush(const Logger* info_log);

#define _FILE strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__

#define LOG_FMT "%s:%d] "

#define LOGV(level, info_log, message, args...) \
  Logv(level, info_log, LOG_FMT message, _FILE, __LINE__, ##args)

void Logv(const int log_level, Logger* info_log,
         const char* format, ...);

extern const char* LEVEL_TAG;
#define gettid() syscall(SYS_gettid)
static inline pid_t pid();

#define DEBUG_LEVEL   0x01
#define INFO_LEVEL    0x02
#define WARN_LEVEL    0x03
#define ERROR_LEVEL   0x04
#define FATAL_LEVEL   0x05
#define NONE_LEVEL    0x06

/*
#ifndef LOG_LEVEL
#define LOG_LEVEL NONE_LEVEL
#endif

#define PRINTFUNCTION(format, ...) fprintf(stderr, format, __VA_ARGS__)

#define LOG_FMT "%-7s %s %5d %s:%d] "
#define LOG_ARGS(LOG_TAG) LOG_TAG, timenow(), pid(), _FILE, __LINE__

#define NEWLINE "\n"

#define DEBUG_TAG   "[DEBUG]"
#define INFO_TAG    "[INFO]"
#define WARN_TAG    "[WARN]"
#define ERROR_TAG   "[ERROR]"
#define FATAL_TAG   "[FATAL]"

#if LOG_LEVEL <= DEBUG_LEVEL
#define LOG_DEBUG(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(DEBUG_TAG), ##args)
#else
#define LOG_DEBUG(message, args...)
#endif

#if LOG_LEVEL <= INFO_LEVEL
#define LOG_INFO(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(INFO_TAG), ##args)
#else
#define LOG_INFO(message, args...)
#endif

#if LOG_LEVEL <= WARN_LEVEL
#define LOG_WARN(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(WARN_TAG), ##args)
#else
#define LOG_WARN(message, args...)
#endif

#if LOG_LEVEL <= ERROR_LEVEL
#define LOG_ERROR(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(ERROR_TAG), ##args)
#else
#define LOG_ERROR(message, args...)
#endif

#if LOG_LEVEL <= FATAL_LEVEL
#define LOG_FATAL(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(FATAL_TAG), ##args)
#else
#define LOG_FATAL(message, args...)
#endif

#if LOG_LEVEL <= NONE_LEVEL
#define LOG_IF_ERROR(condition, message, args...) \
  if (condition)                                  \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(ERROR_TAG), ##args)
#else
#define LOG_IF_ERROR(condition, message, args...)
#endif

static inline char *timenow() {
  static char buffer[64];
  struct timeval tv;
  gettimeofday(&tv, NULL);

  snprintf(buffer, 64, "%ld.%06ld", tv.tv_sec, tv.tv_usec);

  return buffer;
}
*/

static inline pid_t pid() {
  return gettid();
}

} // namespace floyd
#endif
