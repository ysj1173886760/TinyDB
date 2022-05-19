/**
 * @file column_value_expression.cpp
 * @author sheep
 * @brief column value expression
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/column_value_expression.h"

namespace TinyDB {

Value ColumnValueExpression::Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const {
    if (tuple_idx_ == 0) {
        TINYDB_ASSERT(tuple_left != nullptr, "logic error");
        return tuple_left->GetValue(schema_, col_idx_);
    } else {
        TINYDB_ASSERT(tuple_right != nullptr, "logic error");
        return tuple_right->GetValue(schema_, col_idx_);
    }
}

}