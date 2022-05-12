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

#include <string>
#include <sstream>

namespace TinyDB {

enum IndexType {
    BPlusTree = 0,
    HashTable = 1,
};

/**
 * @brief 
 * Metadata of index.
 * Metadata maintains the key schema since index it the mapping from key to RID.
 */
class IndexMetadata {
public:
    IndexMetadata() = delete;
    IndexMetadata(std::string index_name, std::string table_name, const Schema *tuple_schema, std::vector<uint32_t> key_attrs, IndexType type)
        : index_name_(index_name), table_name_(table_name), key_attrs_(key_attrs), type_(type) {
        // generate key schema based on tuple schema and key attrs
        key_schema_ = Schema::CopySchema(tuple_schema, key_attrs);
    }

    ~IndexMetadata() {
        delete key_schema_;
    }

    inline const std::string &GetIndexName() const {
        return index_name_;
    }
    
    inline const std::string &GetTableName() const {
        table_name_;
    }

    // TODO: shall we add const here?
    inline Schema *GetKeySchema() const {
        return key_schema_;
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

        std::string type = type_ == BPlusTree ? "BPlusTree" : "HashTable";

        os << "IndexMetadata["
           << "Name = " << index_name_ << ", "
           << "Type = " << type << ", "
           << "TableName = " << table_name_ << "] :: "
           << key_schema_->ToString();

        return os.str();
    }

private:
    std::string index_name_;
    std::string table_name_;
    Schema *key_schema_;
    // this is not necessary since we can generate key_attrs from key_schema and base schema
    const std::vector<uint32_t> key_attrs_;
    IndexType type_;
};

}

#endif