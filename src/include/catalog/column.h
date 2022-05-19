/**
 * @file column.h
 * @author sheep
 * @brief tuple column/attributes
 * @version 0.1
 * @date 2022-05-06
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef COLUMN_H
#define COLUMN_H

#include "type/type.h"
#include "common/macros.h"

#include <string>

namespace TinyDB {

class AbstractExpression;

/**
 * @brief 
 * metadata of a single column in table. we stored column name, column type
 * max length of varchar, column offsets, etc.
 */
class Column {
    friend class Schema;
public:
    /**
     * @brief
     * Non-variable-length constructor for creating a Column
     * @param column_name name of column
     * @param type_id type id
     * @param expr expression used to create this column. thus we can use expression to create the output schema
     */
    Column(std::string column_name, TypeId type_id, const AbstractExpression *expr = nullptr)
        : column_name_(column_name), column_type_(type_id), fixed_length_(GetFixedLength(type_id)), expr_(expr) {
        TINYDB_ASSERT(type_id != TypeId::VARCHAR, "Wrong constructor for VARCHAR type");
    }

    /**
     * @brief
     * variable-length constructor for creating a Column
     * @param column_name name of column
     * @param type_id type id
     * @param length maxium variable length
     * @param expr expression used to create this column.
     */
    Column(std::string column_name, TypeId type_id, uint32_t length, const AbstractExpression *expr = nullptr)
        : column_name_(column_name), column_type_(type_id), fixed_length_(GetFixedLength(type_id)), variable_length_(length), expr_(expr) {
        TINYDB_ASSERT(type_id == TypeId::VARCHAR, "Wrong constructor for non-varlen type");
    }

    std::string GetName() const {
        return column_name_;
    }

    uint32_t GetLength() const {
        if (IsInlined()) {
            return fixed_length_;
        }
        return variable_length_;
    }

    uint32_t GetFixedLength() const {
        return fixed_length_;
    }

    uint32_t GetVariableLength() const {
        return variable_length_;
    }

    uint32_t GetOffset() const {
        return column_offset_;
    }

    TypeId GetType() const {
        return column_type_;
    }

    // maybe we can also inline varchar for short length?
    bool IsInlined() const {
        return column_type_ != TypeId::VARCHAR;
    }

    std::string ToString() const;

    const AbstractExpression *GetExpr() const {
        return expr_;
    }

private:
    uint32_t GetFixedLength(TypeId type_id) {
        switch (type_id) {
        case TypeId::VARCHAR:
            // store 4 byte offset pointing to real data
            return sizeof(uint32_t);
        default:
            return Type::GetTypeSize(type_id);
        }
    }

    // column name
    std::string column_name_;
    // column value's type
    TypeId column_type_;
    // size of column stored in tuple
    // for inlined value, it's type size
    // otherwise, it stores the offset pointing to the real value
    uint32_t fixed_length_;
    // for a inlined column, 0
    // otherwise, the length of variable length column
    uint32_t variable_length_{0};
    // column offset in the tuple
    uint32_t column_offset_{0};
    // expression used to create this column
    // FIXME: while we are copying column, we are doing the 
    // shallow copy of the expression, which might introduce potential bugs.
    // find a way to handle this, maybe use a unique_ptr to wrap it?
    const AbstractExpression *expr_;

};

}

#endif