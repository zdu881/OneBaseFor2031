#pragma once
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"
#include "onebase/storage/table/table_heap.h"

namespace onebase {

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  const IndexScanPlanNode *plan_;
  TableInfo *table_info_{nullptr};
  TableHeap::Iterator iter_{nullptr, RID(INVALID_PAGE_ID, 0)};
  TableHeap::Iterator end_{nullptr, RID(INVALID_PAGE_ID, 0)};
};

}  // namespace onebase
