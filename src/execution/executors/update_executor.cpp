#include "onebase/execution/executors/update_executor.h"

namespace onebase {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  has_updated_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    return false;
  }
  has_updated_ = true;

  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  int count = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> new_values;
    new_values.reserve(plan_->GetUpdateExpressions().size());
    for (const auto &expr : plan_->GetUpdateExpressions()) {
      new_values.push_back(expr->Evaluate(&child_tuple, &table_info->schema_));
    }
    Tuple updated(std::move(new_values));
    if (table_info->table_->UpdateTuple(child_rid, updated)) {
      count++;
    }
  }

  *tuple = Tuple({Value(TypeId::INTEGER, count)});
  *rid = RID();
  return true;
}

}  // namespace onebase
