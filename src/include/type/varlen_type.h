/**
 * @file varlen_type.h
 * @author sheep
 * @brief varlen type definition
 * @version 0.1
 * @date 2022-05-06
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef VARLEN_TYPE_H
#define VARLEN_TYPE_H

#include "type/type.h"
#include "type/value.h"

namespace TinyDB {

class VarlenType: public Type {
public:
    explicit VarlenType(TypeId type_id): Type(type_id) {}
    // value has the responsibility of destructing the value, not type
    ~VarlenType() override = default;

    CmpBool CompareEquals(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareNotEquals(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareLessThan(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareLessThanEquals(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareGreaterThan(const Value &lhs, const Value &rhs) const override;
    CmpBool CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const override;

    Value Min(const Value &lhs, const Value &rhs) const override;
    Value Max(const Value &lhs, const Value &rhs) const override;

    // varchar value are always not inlined
    // since we want fixed-length tuple
    bool IsInlined(const Value &val) { return false; }

    // for debug purpose
    std::string ToString(const Value &val) const override;

    // serialize/deserialize for storage
    void SerializeTo(const Value &val, char *storage) const override;
    Value DeserializeFrom(const char *storage) const override;

    Value Copy(const Value &val) const override;

    Value CastAs(const Value &val, TypeId type_id) const override;

    const char *GetData(const Value &val) const {
        return val.value_.varlen_;
    }
    uint32_t GetLength(const Value &val) const {
        return val.len_;
    }
};

}

#endif