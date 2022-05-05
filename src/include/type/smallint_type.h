/**
 * @file smallint_type.h
 * @author sheep
 * @brief smallint
 * @version 0.1
 * @date 2022-05-04
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef SMALLINT_TYPE_H
#define SMALLINT_TYPE_H

#include "type/integer_parent_type.h"

namespace TinyDB {

class SmallintType: public IntegerParentType {
public:
    SmallintType(): IntegerParentType(TypeId::SMALLINT) {}
    ~SmallintType() override = default;

    // implement all of the functions that is pure in base type

    Value Add(const Value &lhs, const Value &rhs) const override;
    Value Subtract(const Value &lhs, const Value &rhs) const override;
    Value Multiply(const Value &lhs, const Value &rhs) const override;
    Value Divide(const Value &lhs, const Value &rhs) const override;
    Value Modulo(const Value &lhs, const Value &rhs) const override;
    Value Sqrt(const Value &val) const override;
    Value OperateNull(const Value &lhs, const Value &rhs) const override;
    bool IsZero(const Value &val) const override;

    // inherit these two from numeric_type
    // Value Min(const Value &left, const Value &right) const override;
    // Value Max(const Value &left, const Value &right) const override;

    CmpBool CompareEquals(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareNotEquals(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareLessThan(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareLessThanEquals(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareGreaterThan(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const override;

    // for debug purpose
    std::string ToString(const Value &val) const override;

    // serialize/deserialize for storage
    void SerializeTo(const Value &val, char *storage) const override;
    Value DeserializeFrom(const char *storage) const override;

    Value Copy(const Value &val) const override;

    Value CastAs(const Value &val, TypeId type_id) const override;

};

}

#endif