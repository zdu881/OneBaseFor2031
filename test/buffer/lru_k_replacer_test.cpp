#include <gtest/gtest.h>

#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

TEST(LRUKReplacerTest, BasicEviction) {
  LRUKReplacer replacer(7, 2);

  replacer.RecordAccess(1);
  replacer.RecordAccess(1);
  replacer.RecordAccess(2);

  replacer.SetEvictable(1, true);
  replacer.SetEvictable(2, true);
  EXPECT_EQ(replacer.Size(), 2u);

  frame_id_t frame = INVALID_FRAME_ID;
  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 2);
  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 1);
  EXPECT_FALSE(replacer.Evict(&frame));
}

TEST(LRUKReplacerTest, RemoveRequiresEvictable) {
  LRUKReplacer replacer(4, 2);

  replacer.RecordAccess(0);
  EXPECT_THROW(replacer.Remove(0), OneBaseException);

  replacer.SetEvictable(0, true);
  EXPECT_NO_THROW(replacer.Remove(0));
  EXPECT_EQ(replacer.Size(), 0u);
}

}  // namespace onebase
