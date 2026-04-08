#include "onebase/storage/page/b_plus_tree_leaf_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeLeafPage<int, RID, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (!comparator(array_[i].first, key)) {
      return i;
    }
  }
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value,
                                         const KeyComparator &comparator) const -> bool {
  int pos = KeyIndex(key, comparator);
  if (pos < GetSize() && !comparator(key, array_[pos].first) && !comparator(array_[pos].first, key)) {
    *value = array_[pos].second;
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                         const KeyComparator &comparator) -> int {
  int pos = KeyIndex(key, comparator);
  if (pos < GetSize() && !comparator(key, array_[pos].first) && !comparator(array_[pos].first, key)) {
    return GetSize();
  }
  for (int i = GetSize(); i > pos; i--) {
    array_[i] = array_[i - 1];
  }
  array_[pos].first = key;
  array_[pos].second = value;
  IncreaseSize(1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key,
                                                        const KeyComparator &comparator) -> int {
  int pos = KeyIndex(key, comparator);
  if (pos >= GetSize() || comparator(key, array_[pos].first) || comparator(array_[pos].first, key)) {
    return GetSize();
  }
  for (int i = pos; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int split = GetSize() / 2;
  int count = GetSize() - split;
  for (int i = 0; i < count; i++) {
    recipient->array_[i] = array_[split + i];
  }
  recipient->SetNextPageId(next_page_id_);
  recipient->SetSize(count);
  SetSize(split);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  int recipient_size = recipient->GetSize();
  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[recipient_size + i] = array_[i];
  }
  recipient->SetNextPageId(next_page_id_);
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  int recipient_size = recipient->GetSize();
  recipient->array_[recipient_size] = array_[0];
  recipient->IncreaseSize(1);
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  int recipient_size = recipient->GetSize();
  for (int i = recipient_size; i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0] = array_[GetSize() - 1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

}  // namespace onebase
