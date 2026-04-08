#include <gtest/gtest.h>

#include <cstdio>

#include "onebase/buffer/page_guard.h"
#include "onebase/storage/disk/disk_manager.h"

namespace onebase {

TEST(PageGuardTest, BasicPageGuardDropNoOpWhenEmpty) {
  BasicPageGuard guard;
  EXPECT_NO_THROW(guard.Drop());
}

TEST(PageGuardTest, BasicGuardUnpinsOnDrop) {
  const std::string db_name = "test_guard_basic.db";
  DiskManager disk_manager(db_name);
  BufferPoolManager bpm(2, &disk_manager);

  page_id_t page_id = INVALID_PAGE_ID;
  auto *page = bpm.NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  {
    BasicPageGuard guard(&bpm, page);
    EXPECT_EQ(guard.GetPageId(), page_id);
  }

  page_id_t second = INVALID_PAGE_ID;
  page_id_t third = INVALID_PAGE_ID;
  ASSERT_NE(bpm.NewPage(&second), nullptr);
  ASSERT_NE(bpm.NewPage(&third), nullptr);
  EXPECT_TRUE(bpm.UnpinPage(second, false));
  EXPECT_TRUE(bpm.UnpinPage(third, false));

  disk_manager.ShutDown();
  std::remove(db_name.c_str());
}

TEST(PageGuardTest, WriteGuardMarksDirtyAndReleasesLatch) {
  const std::string db_name = "test_guard_write.db";
  DiskManager disk_manager(db_name);
  BufferPoolManager bpm(2, &disk_manager);

  page_id_t page_id = INVALID_PAGE_ID;
  auto *page = bpm.NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  {
    WritePageGuard guard(&bpm, page);
    std::snprintf(guard.GetDataMut(), 64, "guard_data");
  }

  auto *fetched = bpm.FetchPage(page_id);
  ASSERT_NE(fetched, nullptr);
  EXPECT_STREQ(fetched->GetData(), "guard_data");
  fetched->RLatch();
  fetched->RUnlatch();
  EXPECT_TRUE(bpm.UnpinPage(page_id, false));

  disk_manager.ShutDown();
  std::remove(db_name.c_str());
}

}  // namespace onebase
