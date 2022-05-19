/**
 * @file comparison_expression.h
 * @author sheep
 * @brief comparison expression
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef COMPARISON_EXPRESSION_H
#define COMPARISON_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace TinyDB {

/**
 * @brief 
 * ComparisonExpression is used to express the comparison operations. e.g. X > Y, X == Y, etc.
 */
class ComparisonExpression : public AbstractExpression {
public:
    ComparisonExpression(ExpressionType type, const AbstractExpression *left, const AbstractExpression *right)
        : AbstractExpression(type, {left, right}, TypeId::BOOLEAN) {
        // check the expression type
        switch (type) {
        // fall though
        case ExpressionType::ComparisonExpression_Equal:
        case ExpressionType::ComparisonExpression_NotEqual:
        case ExpressionType::ComparisonExpression_GreaterThan:
        case ExpressionType::ComparisonExpression_GreaterThanEquals:
        case ExpressionType::ComparisonExpression_LessThan:
        case ExpressionType::ComparisonExpression_LessThanEquals:
            break;
        default:
            TINYDB_ASSERT(false, "Invalid expression type for ComparisonExpression");
        }
    }
    
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;
};

}

#endif