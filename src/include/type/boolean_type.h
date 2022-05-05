/**
 * @file boolean_type.h
 * @author sheep
 * @brief boolean
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef BOOLEAN_TYPE_H
#define BOOLEAN_TYPE_H

#include "type/type.h"

namespace TinyDB {

/**
 * @brief 
 * boolean type. the value is whether 0 or 1. other value is not permitted except for NULL
 * we will use assertion to ensure above condition
 * we only allow comparsion between boolean and varchar to avoid the overhead of casting numeric type to boolean
 * and we only support equal and not equal operator
 */
class BooleanType: public Type {
public:
    BooleanType(): Type(TypeId::BOOLEAN) {}
    ~BooleanType() override = default;

    CmpBool CompareEquals(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareNotEquals(const Value &lhs, const Value &rhs) const override;
    
    // should we support these for boolean?
    // CmpBool CompareLessThan(const Value &lhs, const Value &rhs) const override;
    // CmpBool CompareLessThanEquals(const Value &lhs, const Value &rhs) const override;
    // CmpBool CompareGreaterThan(const Value &lhs, const Value &rhs) const override;
    // CmpBool CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const override;

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