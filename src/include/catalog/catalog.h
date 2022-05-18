/**
 * @file catalog.h
 * @author sheep
 * @brief catalog
 * @version 0.1
 * @date 2022-05-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef CATALOG_H
#define CATALOG_H

#include "storage/table/table_heap.h"
#include "storage/index/index.h"
#include "storage/index/index_builder.h"

#include <unordered_map>
#include <memory>
#include <atomic>
#include <mutex>

namespace TinyDB {

using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

struct TableInfo {
    TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
        : schema_(schema),
          name_(name),
          table_(std::move(table)),
          oid_(oid) {}
    
    Schema schema_;
    std::string name_;
    std::unique_ptr<TableHeap> table_;
    table_oid_t oid_;
};

struct IndexInfo {
    IndexInfo(std::unique_ptr<Index> &&index, index_oid_t index_oid)
        : index_(std::move(index)),
          index_oid_(index_oid) {}

    // index has the index metadata, so we don't need to store additional metadata
    std::unique_ptr<Index> index_;
    index_oid_t index_oid_;
};

/**
 * @brief 
 * In-memory catalog that is designed for executor to use. e.g. get table, get index.
 * TODO: find a way to persist the catalog. i.e. restore catalog every time we restart the database
 */
class Catalog {
public:
    /**
     * @brief 
     * Create in-memory catalog object
     * @param bpm 
     */
    Catalog(BufferPoolManager *bpm)
        : bpm_(bpm) {}

    TableInfo *CreateTable(const std::string &table_name, const Schema &schema) {
        std::lock_guard<std::mutex> guard(latch_);
        TINYDB_ASSERT(table_names_.count(table_name) == 0, "Table name should be unique");
        table_oid_t new_oid = next_table_oid_++;
        table_names_[table_name] = new_oid;
        auto new_table = 
            std::make_unique<TableInfo>(schema,
                                        table_name,
                                        std::make_unique<TableHeap>(bpm_),
                                        new_oid);
        tables_[new_oid] = std::move(new_table);
        return tables_[new_oid].get();
    }

    TableInfo *GetTable(const std::string &table_name) {
        std::lock_guard<std::mutex> guard(latch_);
        if (table_names_.count(table_name) == 0) {
            return nullptr;
        }

        table_oid_t table_oid = table_names_[table_name];
        if (tables_.count(table_oid) == 0) {
            return nullptr;
        }

        return tables_[table_oid].get();
    }

    TableInfo *GetTable(table_oid_t table_oid) {
        std::lock_guard<std::mutex> guard(latch_);
        if (tables_.count(table_oid) == 0) {
            return nullptr;
        }

        return tables_[table_oid].get();
    }

    IndexInfo *CreateIndex(const std::string &index_name,
                           const std::string &table_name,
                           const Schema &tuple_schema,
                           const std::vector<uint32_t> &key_attrs,
                           IndexType type,
                           size_t key_size) {
        std::lock_guard<std::mutex> guard(latch_);
        auto table = GetTable(table_name);
        if (table == nullptr) {
            return nullptr;
        }

        index_oid_t new_oid = next_index_oid_++;

        auto index_metadata = std::make_unique<IndexMetadata> (index_name,
                                                               table_name,
                                                               &tuple_schema,
                                                               key_attrs,
                                                               type,
                                                               key_size);
        // use index builder to build the index based on index metadata
        // the metadata will be stored in index
        auto index = IndexBuilder::Build(std::move(index_metadata), bpm_);

        // then we populate the data into index
        // TODO: should we use another abstraction layer to do the population?
        // i.e. hide the population detail which is decided by storage engine
        for (auto it = table->table_->Begin(); it != table->table_->End(); ++it) {
            index->InsertEntryTupleSchema(*it, it->GetRID());
        }

        auto index_info =
            std::make_unique<IndexInfo>(std::move(index), new_oid);
        indexes_[new_oid] = std::move(index_info);
        index_names_[table_name][index_name] = new_oid;
        return indexes_[new_oid].get();
    }

    IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
        std::lock_guard<std::mutex> guard(latch_);
        if (index_names_.count(table_name) == 0) {
            return nullptr;
        }
        if (index_names_[table_name].count(index_name) == 0) {
            return nullptr;
        }

        // since we are holding the lock, i choose not to reuse the code
        index_oid_t index_oid = index_names_[table_name][index_name];
        if (indexes_.count(index_oid) == 0) {
            return nullptr;
        }
        return indexes_[index_oid].get();
    }

    IndexInfo *GetIndex(index_oid_t index_oid) {
        std::lock_guard<std::mutex> guard(latch_);
        if (indexes_.count(index_oid) == 0) {
            return nullptr;
        }
        return indexes_[index_oid].get();
    }

    std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
        std::lock_guard<std::mutex> guard(latch_);
        std::vector<IndexInfo *> res;
        if (index_names_.count(table_name) == 0) {
            return res;
        }

        for (const auto &indexes: index_names_[table_name]) {
            auto ptr = GetIndex(indexes.second);
            if (ptr != nullptr) {
                res.push_back(ptr);
            }
        }

        return res;
    }

private:
    // bpm
    BufferPoolManager *bpm_;

    // table_oid -> table metadata
    std::unordered_map<table_oid_t, std::unique_ptr<TableInfo>> tables_;

    // table_name -> table_oid
    std::unordered_map<std::string, table_oid_t> table_names_;

    // next table identifier
    table_oid_t next_table_oid_{0};

    // index_oid -> index metadata
    std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;

    // table_name : index_name -> index_oid
    std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;

    // next index identifier
    index_oid_t next_index_oid_{0};

    // short-time latch
    std::mutex latch_;
};

}

#endif