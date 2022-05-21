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
 * Table metadata
 */
struct TableInfo {
    TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
        : schema_(schema),
          name_(name),
          table_(std::move(table)),
          oid_(oid) {}
    
    // table schema
    Schema schema_;
    // table name
    std::string name_;
    // pointer to table page heap
    std::unique_ptr<TableHeap> table_;
    // table oid
    table_oid_t oid_;
    // index_oid -> index metadata
    std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
    // index_name -> index_oid
    std::unordered_map<std::string, index_oid_t> index_names_;
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

        return GetTableHelper(table_names_[table_name]);
    }

    TableInfo *GetTable(table_oid_t table_oid) {
        std::lock_guard<std::mutex> guard(latch_);

        return GetTableHelper(table_oid);
    }

    IndexInfo *CreateIndex(const std::string &index_name,
                           const std::string &table_name,
                           const Schema &tuple_schema,
                           const std::vector<uint32_t> &key_attrs,
                           IndexType type,
                           size_t key_size) {
        std::lock_guard<std::mutex> guard(latch_);
        if (table_names_.count(table_name) == 0) {
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
        auto table = GetTableHelper(table_names_[table_name]);
        for (auto it = table->table_->Begin(); it != table->table_->End(); ++it) {
            index->InsertEntryTupleSchema(*it, it->GetRID());
        }

        auto index_info =
            std::make_unique<IndexInfo>(std::move(index), new_oid);
        table->indexes_[new_oid] = std::move(index_info);
        table->index_names_[index_name] = new_oid;
        return table->indexes_[new_oid].get();
    }

    IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
        std::lock_guard<std::mutex> guard(latch_);
        if (table_names_.count(table_name) == 0) {
            return nullptr;
        }
        auto table = tables_[table_names_[table_name]].get();
        auto it = table->index_names_.find(index_name);
        if (it == table->index_names_.end()) {
            return nullptr;
        }

        return table->indexes_[it->second].get();
    }

    std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
        std::lock_guard<std::mutex> guard(latch_);
        std::vector<IndexInfo *> res;
        if (table_names_.count(table_name) == 0) {
            return res;
        }

        for (const auto &index: tables_[table_names_[table_name]]->indexes_) {
            auto ptr = index.second.get();
            if (ptr != nullptr) {
                res.push_back(ptr);
            }
        }

        return res;
    }

private:
    TableInfo *GetTableHelper(table_oid_t table_oid) {
        if (tables_.count(table_oid) == 0) {
            return nullptr;
        }

        return tables_[table_oid].get();
    }

    // bpm
    BufferPoolManager *bpm_;

    // table_oid -> table metadata
    std::unordered_map<table_oid_t, std::unique_ptr<TableInfo>> tables_;

    // table_name -> table_oid
    std::unordered_map<std::string, table_oid_t> table_names_;

    // next table identifier
    table_oid_t next_table_oid_{0};

    // next index identifier
    index_oid_t next_index_oid_{0};

    // short-time latch
    std::mutex latch_;
};

}

#endif