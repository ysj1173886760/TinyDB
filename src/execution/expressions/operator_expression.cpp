/**
 * @file operator_expression.cpp
 * @author sheep
 * @brief operator expression
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/operator_expression.h"
#include "type/value_factory.h"

namespace TinyDB {

Value OperatorExpression::Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const {
    switch (type_) {
    case ExpressionType::OperatorExpression_NOT: {
        TINYDB_ASSERT(children_.size() == 1, "Invalid children number");
        auto val = children_[0]->Evaluate(tuple_left, tuple_right);
        if (val.IsTrue()) {
            return ValueFactory::GetBooleanValue(false);
        } else if (val.IsFalse()) {
            return ValueFactory::GetBooleanValue(true);
        } else {
            return Type::Null(TypeId::BOOLEAN);
        }
    }
    case ExpressionType::OperatorExpression_EXISTS: {
        TINYDB_ASSERT(children_.size() == 1, "Invalid children number");
        auto val = children_[0]->Evaluate(tuple_left, tuple_right);
        if (val.IsNull()) {
            return ValueFactory::GetBooleanValue(false);
        } else {
            return ValueFactory::GetBooleanValue(true);
        }
    }
    case ExpressionType::OperatorExpression_IS_NOT_NULL: {
        TINYDB_ASSERT(children_.size() == 1, "Invalid children number");
        auto val = children_[0]->Evaluate(tuple_left, tuple_right);
        return ValueFactory::GetBooleanValue(!val.IsNull());
    }
    case ExpressionType::OperatorExpression_IS_NULL: {
        TINYDB_ASSERT(children_.size() == 1, "Invalid children number");
        auto val = children_[0]->Evaluate(tuple_left, tuple_right);
        return ValueFactory::GetBooleanValue(val.IsNull());
    }
    default:
        break;
    }

    TINYDB_ASSERT(children_.size() == 2, "Invalid children number");
    auto val_left = children_[0]->Evaluate(tuple_left, tuple_right);
    auto val_right = children_[1]->Evaluate(tuple_left, tuple_right);

    switch (type_) {
    case ExpressionType::OperatorExpression_Add:
        return val_left.Add(val_right);
    case ExpressionType::OperatorExpression_Subtract:
        return val_left.Subtract(val_right);
    case ExpressionType::OperatorExpression_Multiply:
        return val_left.Multiply(val_right);
    case ExpressionType::OperatorExpression_Divide:
        return val_left.Divide(val_right);
    case ExpressionType::OperatorExpression_Modulo:
        return val_left.Modulo(val_right);
    case ExpressionType::OperatorExpression_Min:
        return val_left.Min(val_right);
    case ExpressionType::OperatorExpression_Max:
        return val_left.Max(val_right);
    default:
        TINYDB_ASSERT(false, "invalid type");
    }
}

}