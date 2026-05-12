#include <gtest/gtest.h>

#include "onebase/common/rid.h"
#include "onebase/concurrency/lock_manager.h"
#include "onebase/concurrency/transaction_manager.h"

namespace onebase {

namespace {

void ExpectExclusiveLockIsAvailable(LockManager *lock_manager, const RID &rid, txn_id_t txn_id) {
  Transaction txn(txn_id);
  EXPECT_TRUE(lock_manager->LockExclusive(&txn, rid));
  EXPECT_TRUE(lock_manager->Unlock(&txn, rid));
}

}  // namespace

TEST(TransactionManagerTest, CommitReleasesAllLocks) {
  LockManager lock_manager;
  TransactionManager txn_manager(&lock_manager);

  auto *txn = txn_manager.Begin();
  RID shared_rid_1(1, 1);
  RID shared_rid_2(1, 2);
  RID exclusive_rid_1(2, 1);
  RID exclusive_rid_2(2, 2);

  ASSERT_TRUE(lock_manager.LockShared(txn, shared_rid_1));
  ASSERT_TRUE(lock_manager.LockShared(txn, shared_rid_2));
  ASSERT_TRUE(lock_manager.LockExclusive(txn, exclusive_rid_1));
  ASSERT_TRUE(lock_manager.LockExclusive(txn, exclusive_rid_2));

  txn_manager.Commit(txn);

  EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED);
  EXPECT_TRUE(txn->GetSharedLockSet()->empty());
  EXPECT_TRUE(txn->GetExclusiveLockSet()->empty());

  ExpectExclusiveLockIsAvailable(&lock_manager, shared_rid_1, 100);
  ExpectExclusiveLockIsAvailable(&lock_manager, shared_rid_2, 101);
  ExpectExclusiveLockIsAvailable(&lock_manager, exclusive_rid_1, 102);
  ExpectExclusiveLockIsAvailable(&lock_manager, exclusive_rid_2, 103);
}

TEST(TransactionManagerTest, AbortReleasesAllLocks) {
  LockManager lock_manager;
  TransactionManager txn_manager(&lock_manager);

  auto *txn = txn_manager.Begin();
  RID shared_rid(3, 1);
  RID exclusive_rid(4, 1);

  ASSERT_TRUE(lock_manager.LockShared(txn, shared_rid));
  ASSERT_TRUE(lock_manager.LockExclusive(txn, exclusive_rid));

  txn_manager.Abort(txn);

  EXPECT_EQ(txn->GetState(), TransactionState::ABORTED);
  EXPECT_TRUE(txn->GetSharedLockSet()->empty());
  EXPECT_TRUE(txn->GetExclusiveLockSet()->empty());

  ExpectExclusiveLockIsAvailable(&lock_manager, shared_rid, 200);
  ExpectExclusiveLockIsAvailable(&lock_manager, exclusive_rid, 201);
}

}  // namespace onebase
