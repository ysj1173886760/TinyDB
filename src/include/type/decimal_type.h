/**
 * @file decimal_type.h
 * @author sheep
 * @brief decimal type definition
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef DECIMAL_TYPE_H
#define DECIMAL_TYPE_H

#include "type/numeric_type.h"

#include <string>

namespace TinyDB {

class DecimalType : public NumericType {
public:
    DecimalType(): NumericType(TypeId::DECIMAL) {}
    ~DecimalType() override = default;

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

    // decimal value are always inlined
    bool IsInlined(const Value &val) { return true; }

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