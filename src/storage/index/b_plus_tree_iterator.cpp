#include "onebase/storage/index/b_plus_tree_iterator.h"

#include <functional>

#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/exception.h"
#include "onebase/storage/page/b_plus_tree_leaf_page.h"

namespace onebase {

template class BPlusTreeIterator<int, RID, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
BPLUSTREE_ITERATOR_TYPE::BPlusTreeIterator(page_id_t page_id, int index, BufferPoolManager *bpm)
    : page_id_(page_id), index_(index), bpm_(bpm) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::IsEnd() const -> bool {
  return page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator*() -> const std::pair<KeyType, ValueType> & {
  if (IsEnd()) {
    throw OneBaseException("cannot dereference end iterator", ExceptionType::OUT_OF_RANGE);
  }
  auto *page = bpm_->FetchPage(page_id_);
  if (page == nullptr) {
    throw OneBaseException("failed to fetch leaf page", ExceptionType::BUFFER_FULL);
  }
  auto *leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  if (index_ < 0 || index_ >= leaf->GetSize()) {
    bpm_->UnpinPage(page_id_, false);
    throw OneBaseException("iterator index out of range", ExceptionType::OUT_OF_RANGE);
  }
  current_ = {leaf->KeyAt(index_), leaf->ValueAt(index_)};
  bpm_->UnpinPage(page_id_, false);
  return current_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator++() -> BPlusTreeIterator & {
  if (IsEnd()) {
    return *this;
  }
  auto *page = bpm_->FetchPage(page_id_);
  if (page == nullptr) {
    page_id_ = INVALID_PAGE_ID;
    index_ = 0;
    return *this;
  }
  auto *leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  index_++;
  if (index_ >= leaf->GetSize()) {
    page_id_ = leaf->GetNextPageId();
    index_ = 0;
  }
  bpm_->UnpinPage(page->GetPageId(), false);
  return *this;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator==(const BPlusTreeIterator &other) const -> bool {
  return page_id_ == other.page_id_ && index_ == other.index_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator!=(const BPlusTreeIterator &other) const -> bool {
  return !(*this == other);
}

}  // namespace onebase
