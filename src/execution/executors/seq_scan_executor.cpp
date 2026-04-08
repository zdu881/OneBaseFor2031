#include "onebase/execution/executors/seq_scan_executor.h"

namespace onebase {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = table_info_->table_->Begin();
  end_ = table_info_->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != end_) {
    *tuple = *iter_;
    *rid = tuple->GetRID();
    ++iter_;
    if (plan_->GetPredicate() != nullptr) {
      auto pred = plan_->GetPredicate()->Evaluate(tuple, &table_info_->schema_);
      if (!pred.GetAsBoolean()) {
        continue;
      }
    }
    return true;
  }
  return false;
}

}  // namespace onebase
