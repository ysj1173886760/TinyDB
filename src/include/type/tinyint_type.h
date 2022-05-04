/**
 * @file tinyint_type.h
 * @author sheep
 * @brief tinyint
 * @version 0.1
 * @date 2022-05-03
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TINYINT_TYPE_H
#define TINYINT_TYPE_H

#include "type/integer_parent_type.h"

namespace TinyDB {

class TinyintType: public IntegerParentType {
public:
    TinyintType(): IntegerParentType(TypeId::TINYINT) {}
    ~TinyintType() override = default;

    // implement all of the functions that is pure in base type

    Value Add(const Value &left, const Value &right) const override;
    Value Subtract(const Value &left, const Value &right) const override;
    Value Multiply(const Value &left, const Value &right) const override;
    Value Divide(const Value &left, const Value &right) const override;
    Value Modulo(const Value &left, const Value &right) const override;
    Value Sqrt(const Value &val) const override;
    Value OperateNull(const Value &val, const Value &right) const override;
    bool IsZero(const Value &val) const override;

    // inherit these two from numeric_type
    // Value Min(const Value &left, const Value &right) const override;
    // Value Max(const Value &left, const Value &right) const override;

    CmpBool CompareEquals(const Value &left, const Value &right) const override;
    CmpBool CompareNotEquals(const Value &left, const Value &right) const override;
    CmpBool CompareLessThan(const Value &left, const Value &right) const override;
    CmpBool CompareLessThanEquals(const Value &left, const Value &right) const override;
    CmpBool CompareGreaterThan(const Value &left, const Value &right) const override;
    CmpBool CompareGreaterThanEquals(const Value &left, const Value &right) const override;

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