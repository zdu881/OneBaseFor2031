#include "onebase/execution/executors/index_scan_executor.h"

namespace onebase {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = table_info_->table_->Begin();
  end_ = table_info_->table_->End();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == end_) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRID();
  ++iter_;
  return true;
}

}  // namespace onebase
