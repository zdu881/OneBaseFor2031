#include "onebase/execution/executors/sort_executor.h"
#include <algorithm>

namespace onebase {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  cursor_ = 0;

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }

  const auto &schema = child_executor_->GetOutputSchema();
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    for (const auto &[is_ascending, expr] : plan_->GetOrderBys()) {
      auto left = expr->Evaluate(&a, &schema);
      auto right = expr->Evaluate(&b, &schema);
      if (left.CompareEquals(right).GetAsBoolean()) {
        continue;
      }
      if (is_ascending) {
        return left.CompareLessThan(right).GetAsBoolean();
      }
      return left.CompareGreaterThan(right).GetAsBoolean();
    }
    return false;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= sorted_tuples_.size()) {
    return false;
  }
  *tuple = sorted_tuples_[cursor_++];
  *rid = tuple->GetRID();
  return true;
}

}  // namespace onebase
