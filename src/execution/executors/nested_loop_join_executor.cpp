#include "onebase/execution/executors/nested_loop_join_executor.h"

namespace onebase {

namespace {

auto TupleValues(const Tuple &tuple, const Schema &schema) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(schema.GetColumnCount());
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    values.push_back(tuple.GetValue(&schema, i));
  }
  return values;
}

}  // namespace

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx,
                                                const NestedLoopJoinPlanNode *plan,
                                                std::unique_ptr<AbstractExecutor> left_executor,
                                                std::unique_ptr<AbstractExecutor> right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  result_tuples_.clear();
  cursor_ = 0;

  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();

  std::vector<Tuple> right_tuples;
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples.push_back(tuple);
  }

  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    for (const auto &right_tuple : right_tuples) {
      if (plan_->GetPredicate() != nullptr &&
          !plan_->GetPredicate()->EvaluateJoin(&left_tuple, &left_schema, &right_tuple, &right_schema)
               .GetAsBoolean()) {
        continue;
      }

      auto values = TupleValues(left_tuple, left_schema);
      auto right_values = TupleValues(right_tuple, right_schema);
      values.insert(values.end(), right_values.begin(), right_values.end());
      result_tuples_.emplace_back(std::move(values));
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[cursor_++];
  *rid = tuple->GetRID();
  return true;
}

}  // namespace onebase
