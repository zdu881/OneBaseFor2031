#include "onebase/concurrency/lock_manager.h"
#include "onebase/common/exception.h"

namespace onebase {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto &queue = lock_table_[rid];
  queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  auto iter = std::prev(queue.request_queue_.end());

  queue.cv_.wait(lock, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (const auto &req : queue.request_queue_) {
      if (&req == &(*iter)) {
        continue;
      }
      if (req.granted_ && req.lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    return true;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    queue.request_queue_.erase(iter);
    queue.cv_.notify_all();
    return false;
  }

  iter->granted_ = true;
  txn->GetSharedLockSet()->insert(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto &queue = lock_table_[rid];
  queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto iter = std::prev(queue.request_queue_.end());

  queue.cv_.wait(lock, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (const auto &req : queue.request_queue_) {
      if (&req == &(*iter)) {
        continue;
      }
      if (req.granted_) {
        return false;
      }
    }
    return true;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    queue.request_queue_.erase(iter);
    queue.cv_.notify_all();
    return false;
  }

  iter->granted_ = true;
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto &queue = lock_table_[rid];
  if (queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  queue.upgrading_ = true;

  auto iter = queue.request_queue_.begin();
  for (; iter != queue.request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }
  if (iter == queue.request_queue_.end()) {
    queue.upgrading_ = false;
    return false;
  }

  iter->lock_mode_ = LockMode::EXCLUSIVE;
  iter->granted_ = false;
  txn->GetSharedLockSet()->erase(rid);

  queue.cv_.wait(lock, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (const auto &req : queue.request_queue_) {
      if (&req == &(*iter)) {
        continue;
      }
      if (req.granted_) {
        return false;
      }
    }
    return true;
  });

  queue.upgrading_ = false;
  if (txn->GetState() == TransactionState::ABORTED) {
    queue.request_queue_.erase(iter);
    queue.cv_.notify_all();
    return false;
  }

  iter->granted_ = true;
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  auto table_iter = lock_table_.find(rid);
  if (table_iter == lock_table_.end()) {
    return false;
  }
  auto &queue = table_iter->second;

  bool removed = false;
  for (auto iter = queue.request_queue_.begin(); iter != queue.request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      if (iter->lock_mode_ == LockMode::EXCLUSIVE && !iter->granted_ && queue.upgrading_) {
        queue.upgrading_ = false;
      }
      queue.request_queue_.erase(iter);
      removed = true;
      break;
    }
  }
  if (!removed) {
    return false;
  }

  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  queue.cv_.notify_all();
  return true;
}

}  // namespace onebase
