#include "onebase/execution/executors/seq_scan_executor.h"

namespace onebase {

namespace {

auto MakeOutputTuple(const Tuple &table_tuple, const Schema &schema) -> Tuple {
  std::vector<Value> values;
  values.reserve(schema.GetColumnCount());
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    values.push_back(table_tuple.GetValue(&schema, i));
  }
  Tuple output(std::move(values));
  output.SetRID(table_tuple.GetRID());
  return output;
}

}  // namespace

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = table_info_->table_->Begin();
  end_ = table_info_->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != end_) {
    Tuple table_tuple = *iter_;
    *rid = table_tuple.GetRID();
    ++iter_;
    if (plan_->GetPredicate() != nullptr) {
      auto pred = plan_->GetPredicate()->Evaluate(&table_tuple, &table_info_->schema_);
      if (!pred.GetAsBoolean()) {
        continue;
      }
    }
    *tuple = MakeOutputTuple(table_tuple, table_info_->schema_);
    return true;
  }
  return false;
}

}  // namespace onebase
