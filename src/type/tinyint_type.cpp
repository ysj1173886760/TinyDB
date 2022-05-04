/**
 * @file tinyint_type.cpp
 * @author sheep
 * @brief tinyint implementation
 * @version 0.1
 * @date 2022-05-03
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TINYINT_TYPE_CPP
#define TINYINT_TYPE_CPP

#include "type/tinyint_type.h"
#include "common/macros.h"

#include <string>
#include <assert.h>

namespace TinyDB {

// helper macro
// maybe we can convert varchar to some bigger type?
#define TINYINT_COMPARE_FUNC(OP)                                        \
    switch (rhs.GetTypeId()) {                                          \
    case TypeId::TINYINT:                                               \
        return GetCmpBool(lhs.value_.tinyint_ OP rhs.value_.tinyint_);  \
    case TypeId::SMALLINT:                                              \
        return GetCmpBool(lhs.value_.tinyint_ OP rhs.value_.smallint_); \
    case TypeId::INTEGER:                                               \
        return GetCmpBool(lhs.value_.tinyint_ OP rhs.value_.integer_);  \
    case TypeId::BIGINT:                                                \
        return GetCmpBool(lhs.value_.tinyint_ OP rhs.value_.bigint_);   \
    case TypeId::DECIMAL:                                               \
        return GetCmpBool(lhs.value_.tinyint_ OP rhs.value_.decimal_);  \
    case TypeId::VARCHAR: {                                             \
        auto val = rhs.CastAs(TypeId::TINYINT);                         \
        return GetCmpBool(lhs.value_.tinyint_ OP val.value_.tinyint_);  \
    }                                                                   \
    default:                                                            \
        break;                                                          \
    }

// method stands for our helper function defined in parent class
// since we didn't implement helper function for decimal type, so we use OP
// here to calc the value directly
#define TINYINT_MODIFY_FUNC(METHOD, OP)                                             \
    switch (rhs.GetTypeId()) {                                                      \
    case TypeId::TINYINT:                                                           \
        return METHOD<int8_t, int8_t>(lhs, rhs);                                    \
    case TypeId::SMALLINT:                                                          \
        return METHOD<int8_t, int16_t>(lhs, rhs);                                   \
    case TypeId::INTEGER:                                                           \
        return METHOD<int8_t, int32_t>(lhs, rhs);                                   \
    case TypeId::BIGINT:                                                            \
        return METHOD<int8_t, int64_t>(lhs, rhs);                                   \
    case TypeId::DECIMAL:                                                           \
        return Value(TypeId::DECIMAL, lhs.value_.tinyint_ OP rhs.value_.decimal_);  \
    case TypeId::VARCHAR: {                                                         \
        auto val = rhs.CastAs(TypeId::TINYINT);                                     \
        return METHOD<int8_t, int8_t>(lhs, val);                                    \
    }                                                                               \
    default:                                                                        \
        break;                                                                      \
    }

bool TinyintType::IsZero(const Value &val) const {
    return val.value_.tinyint_ == 0;
}

Value TinyintType::Add(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));
    
    if (lhs.IsNull() || rhs.IsNull()) {
        // should we return null directly
        return lhs.OperateNull(rhs);
    }

    TINYINT_MODIFY_FUNC(AddValue, +);

    UNREACHABLE("type error");
}

Value TinyintType::Subtract(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    TINYINT_MODIFY_FUNC(SubtractValue, -);

    UNREACHABLE("type error");
}

Value TinyintType::Multiply(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    TINYINT_MODIFY_FUNC(MultiplyValue, *);

    UNREACHABLE("type error");
}

Value TinyintType::Divide(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    if (rhs.IsZero()) {
        THROW_DIVIDE_BY_ZERO_EXCEPTION("Division by zero");
    }

    TINYINT_MODIFY_FUNC(DivideValue, /);

    UNREACHABLE("type error");
}

Value TinyintType::Modulo(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    if (rhs.IsZero()) {
        THROW_DIVIDE_BY_ZERO_EXCEPTION("Division by zero");
    }

    // because % can only applied on integer
    switch (rhs.GetTypeId()) {                                                    
    case TypeId::TINYINT:                                                         
        return ModuloValue<int8_t, int8_t>(lhs, rhs);                                  
    case TypeId::SMALLINT:                                                        
        return ModuloValue<int8_t, int16_t>(lhs, rhs);                                 
    case TypeId::INTEGER:                                                         
        return ModuloValue<int8_t, int32_t>(lhs, rhs);                                 
    case TypeId::BIGINT:                                                          
        return ModuloValue<int8_t, int64_t>(lhs, rhs);                                 
    case TypeId::DECIMAL:                                                         
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.tinyint_, rhs.value_.decimal_));
    case TypeId::VARCHAR: {                                                       
        auto val = rhs.CastAs(TypeId::TINYINT);                                   
        return ModuloValue<int8_t, int8_t>(lhs, val);                                  
    }                                                                             
    default:                                                                      
        break;                                                                    
    }

    UNREACHABLE("type error");
}

Value TinyintType::Sqrt(const Value &val) const {
    assert(val.CheckInteger());
    
    if (val.IsNull()) {
        // should we return decimal or integer?
        return Value(TypeId::DECIMAL, TINYDB_DECIMAL_NULL);
    }
    if (val.value_.tinyint_ < 0) {
        THROW_DECIMAL_EXCEPTION("trying to apply sqrt on negative number");
    }

    return Value(TypeId::DECIMAL, std::sqrt(val.value_.tinyint_));
}

// TODO: figure out what type should we really return
// i think return tinyint_null should be ok?
Value TinyintType::OperateNull(const Value &left, const Value &rhs) const {
    switch (rhs.GetTypeId()) {
    case TypeId::TINYINT:
        // not sure what type should we return while using varchar
        // since normal operation will return tinyint, so currently we will return tinyint
    case TypeId::VARCHAR:
        return Value(TypeId::TINYINT, static_cast<int8_t>(TINYDB_INT8_NULL));
    case TypeId::SMALLINT:
        return Value(TypeId::SMALLINT, static_cast<int16_t>(TINYDB_INT16_NULL));
    case TypeId::INTEGER:
        return Value(TypeId::BIGINT, static_cast<int32_t>(TINYDB_INT32_NULL));
    case TypeId::BIGINT:
        return Value(TypeId::DECIMAL, static_cast<int64_t>(TINYDB_INT64_NULL));
    case TypeId::DECIMAL:
        return Value(TypeId::DECIMAL, static_cast<double>(TINYDB_DECIMAL_NULL));
    default:
        break;
    }

    UNREACHABLE("type error");
}

// code reuse is zero
// i'm nearly destroyed while writng this code
// it makes me feel like i'm just a coding machine

CmpBool TinyintType::CompareEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(==);

    UNREACHABLE("type error");
}

CmpBool TinyintType::CompareNotEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(!=);

    UNREACHABLE("type error");
}

CmpBool TinyintType::CompareLessThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(<);

    UNREACHABLE("type error");
}

CmpBool TinyintType::CompareLessThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(<=);

    UNREACHABLE("type error");
}

CmpBool TinyintType::CompareGreaterThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(>);

    UNREACHABLE("type error");
}

CmpBool TinyintType::CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(>=);

    UNREACHABLE("type error");
}

std::string TinyintType::ToString(const Value &val) const {
    assert(val.CheckInteger());
    if (val.IsNull()) {
        return "tinyint_null";
    }

    return std::to_string(val.value_.tinyint_);
}

// serialize/deserialize for storage
// note that serialize/deserialize has nothing to do with string
// we will serialize those into byte array, instead of some meaningful string

void TinyintType::SerializeTo(const Value &val, char *storage) const {
    *reinterpret_cast<int8_t *>(storage) = val.value_.tinyint_;
}

Value TinyintType::DeserializeFrom(const char *storage) const {
    int8_t val = *reinterpret_cast<const int8_t *>(storage);
    return Value(type_id_, val);
}

Value TinyintType::Copy(const Value &val) const {
    assert(val.CheckInteger());
    return Value(TypeId::TINYINT, val.value_.tinyint_);
}

// TODO: i think we need a abstract function to generate the null value
// corresponding to the type

Value TinyintType::CastAs(const Value &val, const TypeId type_id) const {
    if (val.IsNull()) {
        return Type::Null(type_id);
    }

    switch (type_id) {
    case TypeId::TINYINT:
        return Copy(val);
    case TypeId::SMALLINT:
        return Value(type_id, static_cast<int16_t>(val.value_.tinyint_));
    case TypeId::INTEGER:
        return Value(type_id, static_cast<int32_t>(val.value_.tinyint_));
    case TypeId::BIGINT:
        return Value(type_id, static_cast<int64_t>(val.value_.tinyint_));
    case TypeId::DECIMAL:
        return Value(type_id, static_cast<double>(val.value_.tinyint_));
    case TypeId::VARCHAR:
        return Value(type_id, val.ToString());
    default:
        break;
    }

    UNREACHABLE("cannot cast tinyint to " + Type::TypeToString(type_id));
}


}

#endif