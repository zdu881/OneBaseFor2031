#include "onebase/execution/executors/hash_join_executor.h"

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

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                    std::unique_ptr<AbstractExecutor> left_executor,
                                    std::unique_ptr<AbstractExecutor> right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  hash_table_.clear();
  result_tuples_.clear();
  cursor_ = 0;

  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();

  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    auto key = plan_->GetLeftKeyExpression()->Evaluate(&left_tuple, &left_schema).ToString();
    hash_table_[key].push_back(left_tuple);
  }

  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    auto key = plan_->GetRightKeyExpression()->Evaluate(&right_tuple, &right_schema).ToString();
    auto iter = hash_table_.find(key);
    if (iter == hash_table_.end()) {
      continue;
    }
    for (const auto &left_match : iter->second) {
      auto values = TupleValues(left_match, left_schema);
      auto right_values = TupleValues(right_tuple, right_schema);
      values.insert(values.end(), right_values.begin(), right_values.end());
      result_tuples_.emplace_back(std::move(values));
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[cursor_++];
  *rid = tuple->GetRID();
  return true;
}

}  // namespace onebase
