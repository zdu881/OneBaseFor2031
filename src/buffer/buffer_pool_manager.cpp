#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/exception.h"
#include "onebase/common/logger.h"

namespace onebase {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  *page_id = INVALID_PAGE_ID;

  frame_id_t frame_id;
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }

  PrepareFrame(frame_id);

  auto *page = &pages_[frame_id];
  ResetFrame(page);

  *page_id = disk_manager_->AllocatePage();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;

  page_table_[*page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    auto frame_id = iter->second;
    auto *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  frame_id_t frame_id;
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }

  PrepareFrame(frame_id);

  auto *page = &pages_[frame_id];
  ResetFrame(page);
  disk_manager_->ReadPage(page_id, page->GetData());
  page->page_id_ = page_id;
  page->pin_count_ = 1;

  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  auto *page = &pages_[iter->second];
  if (page->pin_count_ <= 0) {
    return false;
  }

  page->pin_count_--;
  page->is_dirty_ = page->is_dirty_ || is_dirty;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(iter->second, true);
  }
  return true;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }

  auto frame_id = iter->second;
  auto *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  page_table_.erase(iter);
  ResetFrame(page);
  free_list_.push_back(frame_id);
  disk_manager_->DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  auto *page = &pages_[iter->second];
  disk_manager_->WritePage(page->page_id_, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    auto *page = &pages_[frame_id];
    if (page->page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::GetAvailableFrame(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  return replacer_->Evict(frame_id);
}

void BufferPoolManager::ResetFrame(Page *page) {
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
}

void BufferPoolManager::PrepareFrame(frame_id_t frame_id) {
  auto *page = &pages_[frame_id];
  if (page->page_id_ == INVALID_PAGE_ID) {
    return;
  }

  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
  }
  page_table_.erase(page->page_id_);
}

}  // namespace onebase
