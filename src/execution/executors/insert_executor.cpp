#include "onebase/execution/executors/insert_executor.h"

namespace onebase {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  has_inserted_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;

  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  int count = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto inserted = table_info->table_->InsertTuple(child_tuple);
    if (inserted.has_value()) {
      count++;
    }
  }

  *tuple = Tuple({Value(TypeId::INTEGER, count)});
  *rid = RID();
  return true;
}

}  // namespace onebase
