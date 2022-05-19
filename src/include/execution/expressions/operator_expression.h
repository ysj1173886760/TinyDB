/**
 * @file operator_expression.h
 * @author sheep
 * @brief operator expression
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef OPERATOR_EXPRESSION_H
#define OPERATOR_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace TinyDB {

class OperatorExpression : public AbstractExpression {
public:
    OperatorExpression(ExpressionType type, AbstractExpression *left, AbstractExpression *right)
        : AbstractExpression(type, {left, right}, DeduceReturnType(type, left, right)) {}
    
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;

private:
    /**
     * @brief 
     * Deduce the return type based on operator and value type
     * @param type 
     * @param left 
     * @param right 
     * @return TypeId 
     */
    TypeId DeduceReturnType(ExpressionType type, AbstractExpression *left, AbstractExpression *right) {
        switch (type) {
            // fall though
        case ExpressionType::OperatorExpression_NOT:
        case ExpressionType::OperatorExpression_IS_NULL:
        case ExpressionType::OperatorExpression_IS_NOT_NULL:
        case ExpressionType::OperatorExpression_EXISTS:
            return TypeId::BOOLEAN;
        default: {
            // otherwise, since we are ordering the typeid with it's size. i.e. bigger type with bigger type id
            // so we can simply return the bigger type
            auto type = std::max(left->GetReturnType(), right->GetReturnType());
            // decimal is the biggest type that can participate in this kind of operation
            TINYDB_ASSERT(type <= TypeId::DECIMAL, "Invalid type");
            return type;
        }
        }
    }
};

}

#endif