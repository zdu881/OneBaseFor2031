#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "onebase/concurrency/lock_manager.h"
#include "onebase/concurrency/transaction.h"

namespace onebase {

TEST(LockManagerTest, SharedLockCompatibility) {
  LockManager lock_mgr;
  Transaction txn0(0);
  Transaction txn1(1);
  RID rid(0, 0);

  EXPECT_TRUE(lock_mgr.LockShared(&txn0, rid));
  EXPECT_TRUE(lock_mgr.LockShared(&txn1, rid));
  EXPECT_TRUE(txn0.IsSharedLocked(rid));
  EXPECT_TRUE(txn1.IsSharedLocked(rid));

  EXPECT_TRUE(lock_mgr.Unlock(&txn0, rid));
  EXPECT_TRUE(lock_mgr.Unlock(&txn1, rid));
}

TEST(LockManagerTest, ExclusiveBlocksUntilUnlock) {
  LockManager lock_mgr;
  Transaction holder(0);
  Transaction waiter(1);
  RID rid(0, 0);

  ASSERT_TRUE(lock_mgr.LockExclusive(&holder, rid));

  std::promise<bool> acquired;
  std::thread t([&]() { acquired.set_value(lock_mgr.LockShared(&waiter, rid)); });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(waiter.IsSharedLocked(rid));

  EXPECT_TRUE(lock_mgr.Unlock(&holder, rid));
  EXPECT_TRUE(acquired.get_future().get());
  t.join();

  EXPECT_TRUE(waiter.IsSharedLocked(rid));
  EXPECT_TRUE(lock_mgr.Unlock(&waiter, rid));
}

TEST(LockManagerTest, UpgradeAndTwoPL) {
  LockManager lock_mgr;
  Transaction txn(0);
  RID rid1(0, 0);
  RID rid2(0, 1);

  EXPECT_TRUE(lock_mgr.LockShared(&txn, rid1));
  EXPECT_TRUE(lock_mgr.LockUpgrade(&txn, rid1));
  EXPECT_TRUE(txn.IsExclusiveLocked(rid1));
  EXPECT_FALSE(txn.IsSharedLocked(rid1));

  EXPECT_TRUE(lock_mgr.Unlock(&txn, rid1));
  EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  EXPECT_FALSE(lock_mgr.LockShared(&txn, rid2));
  EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
}

}  // namespace onebase
