#include "floyd/src/logger.h"

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include "floyd/include/floyd_options.h"

#include "slash/include/env.h"

namespace floyd {

const char* LEVEL_TAG = " DIWEF ";

//static inline char *timenow() {
//  static char buffer[64];
//  struct timeval tv;
//  gettimeofday(&tv, NULL);
//
//  snprintf(buffer, 64, "%ld.%06ld", tv.tv_sec, tv.tv_usec);
//
//  return buffer;
//}
//
//static inline pid_t pid() {
//  return gettid();
//}

//
// A simple Logger of RocksDB's PosixLogger
//
uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

Logger::Logger(FILE* f)
  : file_(f),
    log_size_(0),
    last_flush_micros_(0),
    flush_pending_(false),
#ifdef NDEBUG
  log_level_(INFO_LEVEL)
#else 
  log_level_(DEBUG_LEVEL)
#endif
{}

Logger::~Logger() {
  fclose(file_);
}

void Logger::Flush() {
  if (flush_pending_) {
    flush_pending_ = false;
    fflush(file_);
  }
  last_flush_micros_ = NowMicros();
}

void Logger::Logv(int log_level, const char* format, va_list ap) {
  // log_level only between Debug to Fatal 
  assert(log_level >= 0 && log_level < 6);
  const uint64_t thread_id = pid();

  // We try twice: the first time with a fixed-size stack allocated buffer,
  // and the second time with a much larger dynamically allocated buffer.
  char buffer[500];
  for (int iter = 0; iter < 2; iter++) {
    char* base;
    int bufsize;
    if (iter == 0) {
      bufsize = sizeof(buffer);
      base = buffer;
    } else {
      bufsize = 65536;
      base = new char[bufsize];
    }
    char* p = base;
    char* limit = base + bufsize;

    struct timeval now_tv;
    gettimeofday(&now_tv, nullptr);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    p += snprintf(p, limit - p,
                  "%c%02d%02d %02d:%02d:%02d.%06d %6lld ",
                  LEVEL_TAG[log_level],
                  t.tm_mon + 1,
                  t.tm_mday,
                  t.tm_hour,
                  t.tm_min,
                  t.tm_sec,
                  static_cast<int>(now_tv.tv_usec),
                  static_cast<long long unsigned int>(thread_id));

    // Print the message
    if (p < limit) {
      va_list backup_ap;
      va_copy(backup_ap, ap);
      p += vsnprintf(p, limit - p, format, backup_ap);
      va_end(backup_ap);
    }

    // Truncate to available space if necessary
    if (p >= limit) {
      if (iter == 0) {
        continue;       // Try again with larger buffer
      } else {
        p = limit - 1;
      }
    }

    // Add newline if necessary
    if (p == base || p[-1] != '\n') {
      *p++ = '\n';
    }

    assert(p <= limit);
    const size_t write_size = p - base;


    size_t sz = fwrite(base, 1, write_size, file_);
    flush_pending_ = true;
    assert(sz == write_size);
    if (sz > 0) {
      log_size_ += write_size;
    }
    uint64_t now_micros = static_cast<uint64_t>(now_tv.tv_sec) * 1000000 +
        now_tv.tv_usec;
    if (now_micros - last_flush_micros_ >= flush_every_seconds_ * 1000000) {
      Flush();
    }
    if (base != buffer) {
      delete[] base;
    }
    break;
  }
}

int NewLogger(const std::string& fname, Logger** result) {
  *result = NULL;
  FILE* f = fopen(fname.c_str(), "w");
  if (f == nullptr) {
    return -1;
  } else {
    int fd = fileno(f);
    fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
    *result = new Logger(f);
    return 0;
  }
}

void Logv(const int log_level, Logger* info_log,
         const char* format, ...) {
  if (info_log && info_log->log_level() <= log_level) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(log_level, format, ap);
    va_end(ap);
  }
}

} // namespace floyd
