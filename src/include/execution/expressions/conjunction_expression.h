/**
 * @file conjunction_expression.h
 * @author sheep
 * @brief conjunction expression
 * @version 0.1
 * @date 2022-05-27
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef CONJUNCTION_EXPRESSION_H
#define CONJUNCTION_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace TinyDB {

/**
 * @brief 
 * ConjunctionExpression. e.g. and, or
 * 
 */
class ConjunctionExpression : public AbstractExpression {
public:
    ConjunctionExpression(ExpressionType type, AbstractExpression *left, AbstractExpression *right)
        : AbstractExpression(type, {left, right}, TypeId::BOOLEAN) {
        TINYDB_ASSERT(left != nullptr, "null child");
        TINYDB_ASSERT(right != nullptr, "null child");
        // check return type
        TINYDB_ASSERT(left->GetReturnType() == TypeId::BOOLEAN, "type mismatch");
        TINYDB_ASSERT(right->GetReturnType() == TypeId::BOOLEAN, "type mismatch");
    }
    
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;
};

}

#endif