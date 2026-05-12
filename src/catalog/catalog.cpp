#include "onebase/catalog/catalog.h"
#include <algorithm>
#include <stdexcept>
#include "onebase/storage/index/b_plus_tree.h"

namespace onebase {

IndexInfo::IndexInfo(Schema key_schema, std::string name, std::string table_name,
                     index_oid_t oid, std::vector<uint32_t> key_attrs)
    : key_schema_(std::move(key_schema)), name_(std::move(name)),
      table_name_(std::move(table_name)), oid_(oid), key_attrs_(std::move(key_attrs)) {}

IndexInfo::~IndexInfo() = default;

auto IndexInfo::LookupInteger(int32_t key) const -> const std::vector<RID> * {
  auto it = int_rid_map_.find(key);
  if (it == int_rid_map_.end()) {
    return nullptr;
  }
  return &it->second;
}

auto IndexInfo::InsertEntry(int32_t key, const RID &rid) -> void {
  auto [entry, is_new_key] = int_rid_map_.try_emplace(key);
  if (int_index_ != nullptr && is_new_key) {
    int_index_->Insert(key, rid);
  }
  entry->second.push_back(rid);
}

auto IndexInfo::RemoveEntry(int32_t key, const RID &rid) -> void {
  auto it = int_rid_map_.find(key);
  if (it == int_rid_map_.end()) {
    return;
  }
  auto &rids = it->second;
  rids.erase(std::remove(rids.begin(), rids.end(), rid), rids.end());
  if (rids.empty()) {
    int_rid_map_.erase(it);
    if (int_index_ != nullptr) {
      int_index_->Remove(key);
    }
  }
}

auto Catalog::CreateTable(const std::string &table_name, const Schema &schema) -> TableInfo * {
  if (table_names_.count(table_name) > 0) {
    return nullptr;  // Table already exists
  }
  auto table_oid = next_table_oid_++;
  auto table_heap = std::make_unique<TableHeap>(bpm_);
  auto table_info = std::make_unique<TableInfo>(schema, table_name, std::move(table_heap), table_oid);
  auto *raw = table_info.get();
  tables_[table_oid] = std::move(table_info);
  table_names_[table_name] = table_oid;
  return raw;
}

auto Catalog::GetTable(const std::string &table_name) const -> TableInfo * {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) { return nullptr; }
  return tables_.at(it->second).get();
}

auto Catalog::GetTable(table_oid_t oid) const -> TableInfo * {
  auto it = tables_.find(oid);
  if (it == tables_.end()) { return nullptr; }
  return it->second.get();
}

auto Catalog::GetAllTables() const -> std::vector<TableInfo *> {
  std::vector<TableInfo *> result;
  result.reserve(tables_.size());
  for (const auto &[oid, info] : tables_) {
    result.push_back(info.get());
  }
  return result;
}

auto Catalog::CreateIndex(const std::string &index_name, const std::string &table_name,
                          const std::vector<uint32_t> &key_attrs) -> IndexInfo * {
  auto *table_info = GetTable(table_name);
  if (table_info == nullptr || index_name.empty() || key_attrs.empty()) { return nullptr; }
  if (GetIndex(index_name, table_name) != nullptr) { return nullptr; }

  // Build key schema from key attributes
  std::vector<Column> key_columns;
  for (auto attr : key_attrs) {
    if (attr >= table_info->schema_.GetColumnCount()) {
      return nullptr;
    }
    key_columns.push_back(table_info->schema_.GetColumn(attr));
  }
  Schema key_schema(key_columns);

  auto index_oid = next_index_oid_++;
  auto index_info = std::make_unique<IndexInfo>(
      std::move(key_schema), index_name, table_name, index_oid, key_attrs);
  if (key_attrs.size() == 1 &&
      table_info->schema_.GetColumn(key_attrs.front()).GetType() == TypeId::INTEGER) {
    index_info->supports_point_lookup_ = true;
    index_info->int_index_ = std::make_unique<BPlusTree<int, RID, std::less<int>>>(
        index_name, bpm_, std::less<int>{});
    for (auto it = table_info->table_->Begin(); it != table_info->table_->End(); ++it) {
      Tuple tuple = *it;
      RID rid = it.GetRID();
      int32_t key = tuple.GetValue(&table_info->schema_, key_attrs.front()).GetAsInteger();
      index_info->InsertEntry(key, rid);
    }
  }
  auto *raw = index_info.get();
  indexes_[index_oid] = std::move(index_info);
  index_names_[table_name][index_name] = index_oid;
  return raw;
}

auto Catalog::DropIndex(const std::string &index_name, const std::string &table_name) -> bool {
  auto table_it = index_names_.find(table_name);
  if (table_it == index_names_.end()) { return false; }
  auto idx_it = table_it->second.find(index_name);
  if (idx_it == table_it->second.end()) { return false; }

  indexes_.erase(idx_it->second);
  table_it->second.erase(idx_it);
  if (table_it->second.empty()) {
    index_names_.erase(table_it);
  }
  return true;
}

auto Catalog::GetIndex(const std::string &index_name, const std::string &table_name) const -> IndexInfo * {
  auto table_it = index_names_.find(table_name);
  if (table_it == index_names_.end()) { return nullptr; }
  auto idx_it = table_it->second.find(index_name);
  if (idx_it == table_it->second.end()) { return nullptr; }
  return indexes_.at(idx_it->second).get();
}

auto Catalog::GetIndex(index_oid_t oid) const -> IndexInfo * {
  auto it = indexes_.find(oid);
  if (it == indexes_.end()) {
    return nullptr;
  }
  return it->second.get();
}

auto Catalog::GetTableIndexes(const std::string &table_name) const -> std::vector<IndexInfo *> {
  std::vector<IndexInfo *> result;
  auto table_it = index_names_.find(table_name);
  if (table_it == index_names_.end()) { return result; }
  for (const auto &[name, oid] : table_it->second) {
    result.push_back(indexes_.at(oid).get());
  }
  return result;
}

auto Catalog::GetAllIndexes() const -> std::vector<IndexInfo *> {
  std::vector<IndexInfo *> result;
  result.reserve(indexes_.size());
  for (const auto &[oid, info] : indexes_) {
    result.push_back(info.get());
  }
  return result;
}

}  // namespace onebase
