#include "onebase/execution/executors/projection_executor.h"

namespace onebase {

ProjectionExecutor::ProjectionExecutor(ExecutorContext *exec_ctx, const ProjectionPlanNode *plan,
                                        std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void ProjectionExecutor::Init() {
  child_executor_->Init();
}

auto ProjectionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_rid;
  if (!child_executor_->Next(&child_tuple, &child_rid)) {
    return false;
  }

  std::vector<Value> values;
  values.reserve(plan_->GetExpressions().size());
  for (const auto &expr : plan_->GetExpressions()) {
    values.push_back(expr->Evaluate(&child_tuple, &child_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(std::move(values));
  *rid = child_rid;
  return true;
}

}  // namespace onebase
