/**
 * @file const_value_expression.h
 * @author sheep
 * @brief constant value expression
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef CONSTANT_VALUE_EXPRESSION_H
#define CONSTANT_VALUE_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace TinyDB {

/**
 * @brief 
 * Stores constant value
 */
class ConstantValueExpression : public AbstractExpression {
public:
    ConstantValueExpression(const Value &val)
        : AbstractExpression(ExpressionType::ConstantValueExpression, {}, val.GetTypeId()),
          val_(val) {}
    
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;

private:
    Value val_;
};

}

#endif