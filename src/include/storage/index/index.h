/**
 * @file index.h
 * @author sheep
 * @brief index
 * @version 0.1
 * @date 2022-05-12
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef INDEX_H
#define INDEX_H

#include "catalog/schema.h"
#include "type/value.h"
#include "storage/table/tuple.h"
#include "storage/index/index_iterator.h"
#include "common/exception.h"

#include <string>
#include <sstream>
#include <memory>
#include <deque>
#include <unordered_set>

namespace TinyDB {

class Page;

enum IndexType {
    BPlusTreeType = 0,
    HashTableType = 1,
};

/**
 * @brief 
 * Metadata of index.
 * Metadata maintains the key schema since index it the mapping from key to RID.
 */
class IndexMetadata {
public:
    IndexMetadata() = delete;
    IndexMetadata(const std::string &index_name, 
                  const std::string &table_name, 
                  const Schema *tuple_schema, 
                  std::vector<uint32_t> key_attrs, 
                  IndexType type, 
                  uint32_t key_size)
        : index_name_(index_name), 
          table_name_(table_name), 
          key_attrs_(key_attrs), 
          type_(type),
          key_size_(key_size) {
        // generate key schema based on tuple schema and key attrs
        key_schema_ = Schema::CopySchema(tuple_schema, key_attrs);
        // copy the tuple schema
        tuple_schema_ = new Schema(*tuple_schema);
    }

    IndexMetadata(IndexMetadata &&other)
        : index_name_(other.index_name_), 
          table_name_(other.table_name_), 
          key_schema_(other.key_schema_),
          tuple_schema_(other.tuple_schema_),
          key_attrs_(other.key_attrs_), 
          type_(other.type_),
          key_size_(other.key_size_) {
        other.key_schema_ = nullptr;
        other.tuple_schema_ = nullptr;
    }

    ~IndexMetadata() {
        delete key_schema_;
        delete tuple_schema_;
    }

    inline const std::string &GetIndexName() const {
        return index_name_;
    }
    
    inline const std::string &GetTableName() const {
        return table_name_;
    }

    // TODO: shall we add const here?
    inline Schema *GetKeySchema() const {
        return key_schema_;
    }

    inline Schema *GetTupleSchema() const {
        return tuple_schema_;
    }

    /**
     * @brief 
     * return the number of columns inside index key
     * @return uint32_t 
     */
    inline uint32_t GetIndexColumnCount() const {
        return key_schema_->GetColumnCount();
    }

    inline const std::vector<uint32_t> &GetKeyAttrs() const {
        return key_attrs_;
    }

    inline IndexType GetIndexType() const {
        return type_;
    }

    // for debugging
    std::string ToString() const {
        std::stringstream os;

        std::string type = type_ == IndexType::BPlusTreeType ? "BPlusTree" : "HashTable";

        os << "IndexMetadata["
           << "Name = " << index_name_ << ", "
           << "Type = " << type << ", "
           << "TableName = " << table_name_ << "] :: "
           << key_schema_->ToString();

        return os.str();
    }

    uint32_t GetKeySize() const {
        return key_size_;
    }

private:
    std::string index_name_;
    std::string table_name_;
    Schema *key_schema_;
    Schema *tuple_schema_;
    // this is not necessary since we can generate key_attrs from key_schema and base schema
    std::vector<uint32_t> key_attrs_;
    IndexType type_;
    uint32_t key_size_;
};

/**
 * @brief 
 * base class for different indices. provide the basic interfaces that every index should implement,
 * which are InsertKey, DeleteKey and ScanKey.
 */
class Index {
public:
    explicit Index(std::unique_ptr<IndexMetadata> metadata)
        : metadata_(std::move(metadata)) {}
    
    virtual ~Index() {}

    inline IndexMetadata *GetMetadata() const {
        return metadata_.get();
    }

    inline int GetIndexColumnCount() const {
        return metadata_->GetIndexColumnCount();
    }
    
    inline const std::string &GetIndexName() const {
        return metadata_->GetIndexName();
    }

    inline Schema *GetKeySchema() const {
        return metadata_->GetKeySchema();
    }

    const std::vector<uint32_t> &GetKeyAttrs() const {
        return metadata_->GetKeyAttrs();
    }

    std::string ToString() const {
        return metadata_->ToString();
    }

    // real operations

    // I wonder do we need to provide some function to insert tuple with table_schema
    // directly? since we can use schema and key_attrs in index_metadata to convert 
    // tuple to key.

    /**
     * @brief 
     * insert an entry into index. 
     * @param key key to inserted. The schema for "key" should be key_schema
     * @param rid value to inserted
     */
    virtual void InsertEntry(const Tuple &key, RID rid) = 0;

    /**
     * @brief 
     * delete an entry
     * @param key target key. The schema for "key" should be key_schema
     * @param rid target rid, for supporting duplicated keys
     */
    virtual void DeleteEntry(const Tuple &key, RID rid) = 0;

    /**
     * @brief 
     * get the rid corresponding to given key
     * @param key target key. The schema for "key" should be key_schema
     * @param result result array, for supporting duplicated keys
     */
    virtual void ScanKey(const Tuple &key, std::vector<RID> *result) = 0;

    /**
     * @brief 
     * Get the general index iterator start from the smallest element
     * @return IndexIterator 
     */
    virtual IndexIterator Begin() {
        THROW_NOT_IMPLEMENTED_EXCEPTION("IndexIterator is not implemented");
    }

    /**
     * @brief 
     * Get the general index iterator start from key
     * @param key 
     * @return IndexIterator 
     */
    virtual IndexIterator Begin(const Tuple &key) {
        THROW_NOT_IMPLEMENTED_EXCEPTION("IndexIterator is not implemented");
    }

    // another type of API, which requires the input key have tuple schema 
    // instead of key schema

    /**
     * @brief 
     * insert an entry into index. 
     * @param key key to inserted. The schema for "key" should be tuple_schema
     * @param rid value to inserted
     */
    virtual void InsertEntryTupleSchema(const Tuple &key, RID rid) {
        Tuple tp = key.KeyFromTuple(metadata_->GetTupleSchema(),
                                    metadata_->GetKeySchema(),
                                    metadata_->GetKeyAttrs());
        InsertEntry(tp, std::move(rid));
    }

    /**
     * @brief 
     * delete an entry
     * @param key target key. The schema for "key" should be tuple_schema
     * @param rid target rid, for supporting duplicated keys
     */
    virtual void DeleteEntryTupleSchema(const Tuple &key, RID rid) {
        Tuple tp = key.KeyFromTuple(metadata_->GetTupleSchema(),
                                    metadata_->GetKeySchema(),
                                    metadata_->GetKeyAttrs());
        DeleteEntry(tp, std::move(rid));
    }

    /**
     * @brief 
     * get the rid corresponding to given key
     * @param key target key. The schema for "key" should be tuple_schema
     * @param result result array, for supporting duplicated keys
     */
    virtual void ScanKeyTupleSchema(const Tuple &key, std::vector<RID> *result) {
        Tuple tp = key.KeyFromTuple(metadata_->GetTupleSchema(),
                                    metadata_->GetKeySchema(),
                                    metadata_->GetKeyAttrs());
        ScanKey(tp, result);
    }

    /**
     * @brief 
     * Get the general index iterator start from key
     * @param key schema for "key" should be tuple_schema
     * @return IndexIterator 
     */
    virtual IndexIterator BeginTupleSchema(const Tuple &key) {
        Tuple tp = key.KeyFromTuple(metadata_->GetTupleSchema(),
                                    metadata_->GetKeySchema(),
                                    metadata_->GetKeyAttrs());
        return Begin(tp);
    }

protected:
    std::unique_ptr<IndexMetadata> metadata_;
};

}

#endif