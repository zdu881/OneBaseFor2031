#pragma once
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/catalog/schema.h"
#include "onebase/common/rid.h"
#include "onebase/common/types.h"
#include "onebase/storage/table/table_heap.h"

namespace onebase {

template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTree;

struct TableInfo {
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;

  TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
};

struct IndexInfo {
  Schema key_schema_;
  std::string name_;
  std::string table_name_;
  index_oid_t oid_;
  std::vector<uint32_t> key_attrs_;
  bool supports_point_lookup_{false};
  std::unique_ptr<BPlusTree<int, RID, std::less<int>>> int_index_;
  std::unordered_map<int32_t, std::vector<RID>> int_rid_map_;

  IndexInfo(Schema key_schema, std::string name, std::string table_name,
            index_oid_t oid, std::vector<uint32_t> key_attrs);
  ~IndexInfo();

  auto SupportsPointLookup() const -> bool { return supports_point_lookup_; }
  auto GetLookupAttr() const -> uint32_t { return key_attrs_.front(); }

  auto LookupInteger(int32_t key) const -> const std::vector<RID> *;
  auto InsertEntry(int32_t key, const RID &rid) -> void;
  auto RemoveEntry(int32_t key, const RID &rid) -> void;
};

class Catalog {
 public:
  explicit Catalog(BufferPoolManager *bpm) : bpm_(bpm) {}

  auto CreateTable(const std::string &table_name, const Schema &schema) -> TableInfo *;
  auto GetTable(const std::string &table_name) const -> TableInfo *;
  auto GetTable(table_oid_t oid) const -> TableInfo *;
  auto GetAllTables() const -> std::vector<TableInfo *>;

  auto CreateIndex(const std::string &index_name, const std::string &table_name,
                   const std::vector<uint32_t> &key_attrs) -> IndexInfo *;
  auto DropIndex(const std::string &index_name, const std::string &table_name) -> bool;
  auto GetIndex(const std::string &index_name, const std::string &table_name) const -> IndexInfo *;
  auto GetIndex(index_oid_t oid) const -> IndexInfo *;
  auto GetTableIndexes(const std::string &table_name) const -> std::vector<IndexInfo *>;
  auto GetAllIndexes() const -> std::vector<IndexInfo *>;

 private:
  BufferPoolManager *bpm_;
  table_oid_t next_table_oid_{1};
  index_oid_t next_index_oid_{1};

  std::unordered_map<table_oid_t, std::unique_ptr<TableInfo>> tables_;
  std::unordered_map<std::string, table_oid_t> table_names_;
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
};

}  // namespace onebase
