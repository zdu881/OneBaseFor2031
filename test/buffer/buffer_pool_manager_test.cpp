#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>

#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/storage/disk/disk_manager.h"

namespace onebase {

TEST(BufferPoolManagerTest, NewPageAndFetchCycle) {
  const std::string db_name = "test_bpm_cycle.db";
  DiskManager disk_manager(db_name);
  BufferPoolManager bpm(3, &disk_manager);

  page_id_t page_id = INVALID_PAGE_ID;
  auto *page = bpm.NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  std::snprintf(page->GetData(), 64, "hello_onebase");
  EXPECT_TRUE(bpm.UnpinPage(page_id, true));

  auto *fetched = bpm.FetchPage(page_id);
  ASSERT_NE(fetched, nullptr);
  EXPECT_STREQ(fetched->GetData(), "hello_onebase");
  EXPECT_TRUE(bpm.UnpinPage(page_id, false));

  disk_manager.ShutDown();
  std::remove(db_name.c_str());
}

TEST(BufferPoolManagerTest, EvictionWhenPoolFull) {
  const std::string db_name = "test_bpm_evict.db";
  DiskManager disk_manager(db_name);
  BufferPoolManager bpm(2, &disk_manager);

  page_id_t first = INVALID_PAGE_ID;
  page_id_t second = INVALID_PAGE_ID;
  page_id_t third = INVALID_PAGE_ID;
  ASSERT_NE(bpm.NewPage(&first), nullptr);
  ASSERT_NE(bpm.NewPage(&second), nullptr);
  EXPECT_EQ(bpm.NewPage(&third), nullptr);

  EXPECT_TRUE(bpm.UnpinPage(first, false));
  ASSERT_NE(bpm.NewPage(&third), nullptr);
  EXPECT_TRUE(bpm.UnpinPage(second, false));
  EXPECT_TRUE(bpm.UnpinPage(third, false));

  disk_manager.ShutDown();
  std::remove(db_name.c_str());
}

}  // namespace onebase
