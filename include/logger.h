#ifndef __LOGGER_H__
#define __LOGGER_H__

#include <time.h>
#include <string.h>

static inline char *timenow();

#define _FILE strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__

#define DEBUG 0x04
#define INFO 0x03
#define WARNING 0x02
#define ERROR 0x01
#define NONE 0x00

#ifndef LOG_LEVEL
#define LOG_LEVEL NONE
#endif

#define PRINTFUNCTION(format, ...) fprintf(stderr, format, __VA_ARGS__)

#define LOG_FMT "%-7s %s %s:%d] "
#define LOG_ARGS(LOG_TAG) LOG_TAG, timenow(), _FILE, __LINE__

#define NEWLINE "\n"

#define DEBUG_TAG "[DEBUG]"
#define INFO_TAG "[INFO]"
#define WARN_TAG "[WARN]"
#define ERROR_TAG "[ERROR]"

#if LOG_LEVEL >= DEBUG
#define LOG_DEBUG(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(DEBUG_TAG), ##args)
#else
#define LOG_DEBUG(message, args...)
#endif

#if LOG_LEVEL >= INFO
#define LOG_INFO(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(INFO_TAG), ##args)
#else
#define LOG_INFO(message, args...)
#endif

#if LOG_LEVEL >= WARNING
#define LOG_WARN(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(WARN_TAG), ##args)
#else
#define LOG_WARN(message, args...)
#endif

#if LOG_LEVEL >= ERROR
#define LOG_ERROR(message, args...) \
  PRINTFUNCTION(LOG_FMT message NEWLINE, LOG_ARGS(ERROR_TAG), ##args)
#else
#define LOG_ERROR(message, args...)
#endif

#if LOG_LEVEL >= NONE
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

#endif
