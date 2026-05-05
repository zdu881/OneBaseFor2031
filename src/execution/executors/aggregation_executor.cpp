#include "onebase/execution/executors/aggregation_executor.h"

#include <unordered_map>

namespace onebase {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                          std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  result_tuples_.clear();
  cursor_ = 0;

  struct AggState {
    std::vector<Value> group_vals;
    std::vector<Value> agg_vals;
    std::vector<bool> initialized;
  };

  std::unordered_map<std::string, size_t> group_map;
  std::vector<AggState> states;
  const auto &schema = child_executor_->GetOutputSchema();

  auto make_group_key = [](const std::vector<Value> &values) {
    std::string key;
    for (const auto &value : values) {
      key.append(value.ToString());
      key.push_back('\x1f');
    }
    return key;
  };

  Tuple tuple;
  RID rid;
  bool saw_input = false;
  while (child_executor_->Next(&tuple, &rid)) {
    saw_input = true;
    std::vector<Value> group_vals;
    group_vals.reserve(plan_->GetGroupBys().size());
    for (const auto &expr : plan_->GetGroupBys()) {
      group_vals.push_back(expr->Evaluate(&tuple, &schema));
    }
    auto key = make_group_key(group_vals);

    size_t idx;
    auto it = group_map.find(key);
    if (it == group_map.end()) {
      idx = states.size();
      group_map.emplace(key, idx);
      AggState state;
      state.group_vals = group_vals;
      state.agg_vals.reserve(plan_->GetAggregateTypes().size());
      state.initialized.assign(plan_->GetAggregateTypes().size(), false);
      for (auto agg_type : plan_->GetAggregateTypes()) {
        switch (agg_type) {
          case AggregationType::CountStarAggregate:
          case AggregationType::CountAggregate:
            state.agg_vals.emplace_back(TypeId::INTEGER, 0);
            break;
          case AggregationType::SumAggregate:
          case AggregationType::MinAggregate:
          case AggregationType::MaxAggregate:
            state.agg_vals.emplace_back(TypeId::INTEGER);
            break;
        }
      }
      states.push_back(std::move(state));
    } else {
      idx = it->second;
    }

    auto &state = states[idx];
    for (size_t i = 0; i < plan_->GetAggregateTypes().size(); i++) {
      auto agg_type = plan_->GetAggregateTypes()[i];
      if (agg_type == AggregationType::CountStarAggregate) {
        state.agg_vals[i] = state.agg_vals[i].Add(Value(TypeId::INTEGER, 1));
        continue;
      }

      auto input_val = plan_->GetAggregates()[i]->Evaluate(&tuple, &schema);
      if (input_val.IsNull()) {
        continue;
      }
      switch (agg_type) {
        case AggregationType::CountAggregate:
          state.agg_vals[i] = state.agg_vals[i].Add(Value(TypeId::INTEGER, 1));
          break;
        case AggregationType::SumAggregate:
          if (!state.initialized[i]) {
            state.agg_vals[i] = input_val;
          } else {
            state.agg_vals[i] = state.agg_vals[i].Add(input_val);
          }
          state.initialized[i] = true;
          break;
        case AggregationType::MinAggregate:
          if (!state.initialized[i] || input_val.CompareLessThan(state.agg_vals[i]).GetAsBoolean()) {
            state.agg_vals[i] = input_val;
          }
          state.initialized[i] = true;
          break;
        case AggregationType::MaxAggregate:
          if (!state.initialized[i] || input_val.CompareGreaterThan(state.agg_vals[i]).GetAsBoolean()) {
            state.agg_vals[i] = input_val;
          }
          state.initialized[i] = true;
          break;
        case AggregationType::CountStarAggregate:
          break;
      }
    }
  }

  if (!saw_input && plan_->GetGroupBys().empty()) {
    std::vector<Value> values;
    for (auto agg_type : plan_->GetAggregateTypes()) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
        case AggregationType::CountAggregate:
          values.emplace_back(TypeId::INTEGER, 0);
          break;
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          values.emplace_back(TypeId::INTEGER);
          break;
      }
    }
    result_tuples_.emplace_back(std::move(values));
    return;
  }

  for (auto &state : states) {
    std::vector<Value> values = state.group_vals;
    values.insert(values.end(), state.agg_vals.begin(), state.agg_vals.end());
    result_tuples_.emplace_back(std::move(values));
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[cursor_++];
  *rid = tuple->GetRID();
  return true;
}

}  // namespace onebase
