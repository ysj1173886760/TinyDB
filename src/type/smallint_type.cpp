/**
 * @file smallint_type.cpp
 * @author sheep
 * @brief smallint implementation
 * @version 0.1
 * @date 2022-05-04
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef SMALLINT_TYPE_CPP
#define SMALLINT_TYPE_CPP

#include "type/smallint_type.h"
#include "common/macros.h"

#include <string>
#include <assert.h>

namespace TinyDB {

// helper macro
#define SMALLINT_COMPARE_FUNC(OP)                                           \
    switch (rhs.GetTypeId()) {                                              \
    case TypeId::TINYINT:                                                   \
        return GetCmpBool(lhs.value_.smallint_ OP rhs.value_.tinyint_);     \
    case TypeId::SMALLINT:                                                  \
        return GetCmpBool(lhs.value_.smallint_ OP rhs.value_.smallint_);    \
    case TypeId::INTEGER:                                                   \
        return GetCmpBool(lhs.value_.smallint_ OP rhs.value_.integer_);     \
    case TypeId::BIGINT:                                                    \
        return GetCmpBool(lhs.value_.smallint_ OP rhs.value_.bigint_);      \
    case TypeId::DECIMAL:                                                   \
        return GetCmpBool(lhs.value_.smallint_ OP rhs.value_.decimal_);     \
    case TypeId::VARCHAR: {                                                 \
        auto val = rhs.CastAs(TypeId::SMALLINT);                            \
        return GetCmpBool(lhs.value_.smallint_ OP val.value_.smallint_);    \
    }                                                                       \
    default:                                                                \
        break;                                                              \
    }

// method stands for our helper function defined in parent class
#define SMALLINT_MODIFY_FUNC(METHOD, OP)                                                \
    switch (rhs.GetTypeId()) {                                                          \
    case TypeId::TINYINT:                                                               \
        return METHOD<int16_t, int8_t>(lhs, rhs);                                       \
    case TypeId::SMALLINT:                                                              \
        return METHOD<int16_t, int16_t>(lhs, rhs);                                      \
    case TypeId::INTEGER:                                                               \
        return METHOD<int16_t, int32_t>(lhs, rhs);                                      \
    case TypeId::BIGINT:                                                                \
        return METHOD<int16_t, int64_t>(lhs, rhs);                                      \
    case TypeId::DECIMAL:                                                               \
        return Value(TypeId::DECIMAL, lhs.value_.smallint_ OP rhs.value_.decimal_);     \
    case TypeId::VARCHAR: {                                                             \
        auto val = rhs.CastAs(TypeId::SMALLINT);                                        \
        return METHOD<int16_t, int16_t>(lhs, val);                                      \
    }                                                                                   \
    default:                                                                            \
        break;                                                                          \
    }

bool SmallintType::IsZero(const Value &val) const {
    return val.value_.smallint_ == 0;
}

Value SmallintType::Add(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));
    
    if (lhs.IsNull() || rhs.IsNull()) {
        // should we return null directly
        return lhs.OperateNull(rhs);
    }

    SMALLINT_MODIFY_FUNC(AddValue, +);

    UNREACHABLE("type error");
}

Value SmallintType::Subtract(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    SMALLINT_MODIFY_FUNC(SubtractValue, -);

    UNREACHABLE("type error");
}

Value SmallintType::Multiply(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    SMALLINT_MODIFY_FUNC(MultiplyValue, *);

    UNREACHABLE("type error");
}

Value SmallintType::Divide(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    if (rhs.IsZero()) {
        THROW_DIVIDE_BY_ZERO_EXCEPTION("Division by zero");
    }

    SMALLINT_MODIFY_FUNC(DivideValue, /);

    UNREACHABLE("type error");
}

Value SmallintType::Modulo(const Value &lhs, const Value &rhs) const {
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
        return ModuloValue<int16_t, int8_t>(lhs, rhs);                                  
    case TypeId::SMALLINT:                                                        
        return ModuloValue<int16_t, int16_t>(lhs, rhs);                                 
    case TypeId::INTEGER:                                                         
        return ModuloValue<int16_t, int32_t>(lhs, rhs);                                 
    case TypeId::BIGINT:                                                          
        return ModuloValue<int16_t, int64_t>(lhs, rhs);                                 
    case TypeId::DECIMAL:                                                         
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.smallint_, rhs.value_.decimal_));
    case TypeId::VARCHAR: {                                                       
        auto val = rhs.CastAs(TypeId::SMALLINT);                                   
        return ModuloValue<int16_t, int16_t>(lhs, val);                                  
    }                                                                             
    default:                                                                      
        break;                                                                    
    }

    UNREACHABLE("type error");
}

Value SmallintType::Sqrt(const Value &val) const {
    assert(val.CheckInteger());
    
    if (val.IsNull()) {
        // should we return decimal or integer?
        return Value(TypeId::DECIMAL, TINYDB_DECIMAL_NULL);
    }
    if (val.value_.smallint_ < 0) {
        THROW_DECIMAL_EXCEPTION("trying to apply sqrt on negative number");
    }

    return Value(TypeId::DECIMAL, std::sqrt(val.value_.smallint_));
}

Value SmallintType::OperateNull(const Value &left, const Value &rhs) const {
    switch (rhs.GetTypeId()) {
    case TypeId::TINYINT:
        return Value(TypeId::TINYINT, static_cast<int8_t>(TINYDB_INT8_NULL));
    case TypeId::VARCHAR:
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

CmpBool SmallintType::CompareEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    SMALLINT_COMPARE_FUNC(==);

    UNREACHABLE("type error");
}

CmpBool SmallintType::CompareNotEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    SMALLINT_COMPARE_FUNC(!=);

    UNREACHABLE("type error");
}

CmpBool SmallintType::CompareLessThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    SMALLINT_COMPARE_FUNC(<);

    UNREACHABLE("type error");
}

CmpBool SmallintType::CompareLessThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    SMALLINT_COMPARE_FUNC(<=);

    UNREACHABLE("type error");
}

CmpBool SmallintType::CompareGreaterThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    SMALLINT_COMPARE_FUNC(>);

    UNREACHABLE("type error");
}

CmpBool SmallintType::CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    SMALLINT_COMPARE_FUNC(>=);

    UNREACHABLE("type error");
}

std::string SmallintType::ToString(const Value &val) const {
    assert(val.CheckInteger());
    if (val.IsNull()) {
        return "smallint_null";
    }

    return std::to_string(val.value_.smallint_);
}

// serialize/deserialize for storage
// note that serialize/deserialize has nothing to do with string
// we will serialize those into byte array, instead of some meaningful string

void SmallintType::SerializeTo(const Value &val, char *storage) const {
    *reinterpret_cast<int16_t *>(storage) = val.value_.smallint_;
}

Value SmallintType::DeserializeFrom(const char *storage) const {
    int8_t val = *reinterpret_cast<const int16_t *>(storage);
    return Value(type_id_, val);
}

Value SmallintType::Copy(const Value &val) const {
    assert(val.CheckInteger());
    return Value(TypeId::SMALLINT, val.value_.smallint_);
}

Value SmallintType::CastAs(const Value &val, const TypeId type_id) const {
    if (val.IsNull()) {
        return Type::Null(type_id);
    }

    switch (type_id) {
    case TypeId::TINYINT:
        return Value(type_id, static_cast<int8_t>(val.value_.smallint_));
    case TypeId::SMALLINT:
        return Copy(val);
    case TypeId::INTEGER:
        return Value(type_id, static_cast<int32_t>(val.value_.smallint_));
    case TypeId::BIGINT:
        return Value(type_id, static_cast<int64_t>(val.value_.smallint_));
    case TypeId::DECIMAL:
        return Value(type_id, static_cast<double>(val.value_.smallint_));
    case TypeId::VARCHAR:
        return Value(type_id, val.ToString());
    default:
        break;
    }

    UNREACHABLE("cannot cast smallint to " + Type::TypeToString(type_id));
}


}

#endif