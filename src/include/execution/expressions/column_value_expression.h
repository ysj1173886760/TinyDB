/**
 * @file column_value_expression.h
 * @author sheep
 * @brief column value expression
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef COLUMN_VALUE_EXPRESSION
#define COLUMN_VALUE_EXPRESSION

#include "execution/expressions/abstract_expression.h"

namespace TinyDB {

/**
 * @brief 
 * ColumnValueExpression is used to generate the value based on
 * value index and tuple index.
 */
class ColumnValueExpression : public AbstractExpression {
public:
    ColumnValueExpression(TypeId ret_type, uint32_t tuple_idx, uint32_t col_idx, Schema *schema)
        : AbstractExpression(ExpressionType::ColumnValueExpression, {}, ret_type),
          tuple_idx_(tuple_idx),
          col_idx_(col_idx),
          schema_(schema) {
        // check the value type based on schema and col idx
        TINYDB_ASSERT(schema->GetColumn(col_idx).GetType() == ret_type, "logic error");
    }
        
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;
    
    inline uint32_t GetTupleIdx() const {
        return tuple_idx_;
    }

    inline uint32_t GetColIdx() const {
        return col_idx_;
    }

private:
    // tuple idx, 0 for left tuple, 1 for right tuple
    uint32_t tuple_idx_;
    // index of column that we want to get
    uint32_t col_idx_;
    // tuple schema
    Schema *schema_;
};

}

#endif