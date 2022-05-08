/**
 * @file schema.h
 * @author sheep
 * @brief schema
 * @version 0.1
 * @date 2022-05-06
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef SCHEMA_H
#define SCHEMA_H

#include "catalog/column.h"

#include <assert.h>
#include <vector>

namespace TinyDB {

/**
 * @brief 
 * table schema, contains the column information of this table
 */
class Schema {
public:
    /**
     * @brief construct schema using columns. order is matter
     * @param columns 
     */
    explicit Schema(const std::vector<Column> &columns);

    /**
     * @brief 
     * construct a new schema based on current schema and selected columns
     * @param from current schema
     * @param column_indices indices of columns you want to include in new schema
     * @return Schema* 
     */
    static Schema* CopySchema(const Schema *from, const std::vector<uint32_t> &column_indices);

    // some helper functions

    // return the reference of all columns in this schema
    // should be used carefully
    inline const std::vector<Column> &GetColumns() const {
        return columns_;
    }

    // return the specific column corresponding to col_idx
    inline const Column &GetColumn(const uint32_t col_idx) const {
        assert(col_idx < columns_.size());
        return columns_[col_idx];
    }

    // get the col idx corresponding to col_name
    // return UINT32_MAX when we didn't find this column
    uint32_t GetColIdx(const std::string &col_name) const {
        for (size_t i = 0; i < columns_.size(); i++) {
            if (columns_[i].GetName() == col_name) {
                return i;
            }
        }
        // don't throw exception.
        // We rather handle the error than throw exception everywhere
        return UINT32_MAX;
    }

    std::string ToString() const;

    inline const std::vector<uint32_t> &GetUninlinedColumns() const {
        return uninlined_columns_;
    }

    inline uint32_t GetColumnCount() const {
        return static_cast<uint32_t> (columns_.size());
    }

    inline uint32_t GetUninlinedColumnCount() const {
        return static_cast<uint32_t> (uninlined_columns_.size());
    }

    // return the length of tuple. count by bytes
    inline uint32_t GetLength() const {
        return length_;
    }

    /**
     * @brief 
     * return whether tuple is inlined
     */
    inline bool IsInlined() const {
        return is_tuple_inlined_;
    }

    /**
     * @brief 
     * generate key attributes from it's father schema.
     * i.e. fetch the column indices that will generate current schema based on it's father schema
     * used in KeyFromTuple in tuple.h
     * @param schema 
     * @return std::vector<uint32_t> 
     */
    std::vector<uint32_t> GenerateKeyAttrs(const Schema *schema) const;

private:
    // length of one tuple, count by bytes
    uint32_t length_;

    // all columns in this table
    std::vector<Column> columns_;

    // true if all tuples are inlined, false otherwise
    bool is_tuple_inlined_;

    // contains indices of all uninlined columns
    std::vector<uint32_t> uninlined_columns_;
};

}

#endif