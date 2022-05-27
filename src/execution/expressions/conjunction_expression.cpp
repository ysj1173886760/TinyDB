/**
 * @file conjunction_expression.cpp
 * @author sheep
 * @brief conjunction expression
 * @version 0.1
 * @date 2022-05-27
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/conjunction_expression.h"
#include "type/value_factory.h"

namespace TinyDB {

Value ConjunctionExpression::Evaluate(const Tuple *left, const Tuple *right) const {
    auto vl = children_[0]->Evaluate(left, right);
    auto vr = children_[1]->Evaluate(left, right);
    switch (type_) {
    case ExpressionType::ConjunctionExpression_AND:
        if (vl.IsTrue() && vr.IsTrue()) {
            return ValueFactory::GetBooleanValue(true);
        }
        if (vl.IsFalse() || vr.IsFalse()) {
            return ValueFactory::GetBooleanValue(false);
        }
        return Type::Null(TypeId::BOOLEAN);
    case ExpressionType::ConjunctionExpression_OR:
        if (vl.IsFalse() && vr.IsFalse()) {
            return ValueFactory::GetBooleanValue(false);
        }
        if (vl.IsTrue() || vr.IsTrue()) {
            return ValueFactory::GetBooleanValue(true);
        }
        return Type::Null(TypeId::BOOLEAN);
    default:
        TINYDB_ASSERT(false, "Invalid type");
    }
}
/**
 * @brief 
 * table
TRUE AND TRUE ==> TRUE
TRUE AND FALSE ==> FALSE
TRUE AND NULL ==> NULL
FALSE AND TRUE ==> FALSE
FALSE AND FALSE ==> FALSE
FALSE AND NULL ==> FALSE
NULL AND TRUE ==> NULL
NULL AND FALSE ==> FALSE
NULL AND NULL ==> NULL
TRUE OR TRUE ==> TRUE
TRUE OR FALSE ==> TRUE
TRUE OR NULL ==> TRUE
FALSE OR TRUE ==> TRUE
FALSE OR FALSE ==> FALSE
FALSE OR NULL ==> NULL
NULL OR TRUE ==> TRUE
NULL OR FALSE ==> NULL
NULL OR NULL ==> NULL
 */

}