#ifndef FLOYD_APPLY_H_
#define FLOYD_APPLY_H_

#include "floyd/src/floyd_context.h"

#include "slash/include/slash_status.h"
#include "pink/include/bg_thread.h"
#include "nemo-rocksdb/db_nemo_impl.h"


namespace floyd {
using slash::Status;

class FloydApply {
 public:
  FloydApply(FloydContext* context, rocksdb::DBNemo* db, Log* log);
  ~FloydApply();
  Status ScheduleApply();

 private:
  pink::BGThread* bg_thread_;
  FloydContext* context_;
  rocksdb::DBNemo* db_;
  Log* log_;
  
  static void ApplyStateMachine(void* arg);
  Status Apply(const Entry& log_entry);
};

}

#endif
