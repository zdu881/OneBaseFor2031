#include "onebase/execution/executors/index_scan_executor.h"

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

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  index_info_ = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  matching_rids_.clear();
  cursor_ = 0;

  if (table_info_ == nullptr || index_info_ == nullptr || !index_info_->SupportsPointLookup()) {
    return;
  }

  Tuple dummy;
  auto key_value = plan_->GetLookupKey()->Evaluate(&dummy, &index_info_->key_schema_);
  auto *rids = index_info_->LookupInteger(key_value.GetAsInteger());
  if (rids != nullptr) {
    matching_rids_ = *rids;
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (cursor_ < matching_rids_.size()) {
    RID current_rid = matching_rids_[cursor_++];
    Tuple table_tuple = table_info_->table_->GetTuple(current_rid);
    if (plan_->GetPredicate() != nullptr) {
      auto pred = plan_->GetPredicate()->Evaluate(&table_tuple, &table_info_->schema_);
      if (!pred.GetAsBoolean()) {
        continue;
      }
    }
    *tuple = MakeOutputTuple(table_tuple, table_info_->schema_);
    *rid = current_rid;
    return true;
  }
  return false;
}

}  // namespace onebase
