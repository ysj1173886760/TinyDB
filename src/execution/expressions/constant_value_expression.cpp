/**
 * @file constant_value_expression.cpp
 * @author sheep
 * @brief constant value expression
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/constant_value_expression.h"

namespace TinyDB {

// do we really need to put this into a separate compile unit?
Value ConstantValueExpression::Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const {
    return val_;
}
    
} // namespace TinyDB

