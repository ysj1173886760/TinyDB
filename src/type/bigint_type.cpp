/**
 * @file bigint_type.cpp
 * @author sheep
 * @brief bigint implementation
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/bigint_type.h"
#include "common/macros.h"

#include <string>
#include <assert.h>

namespace TinyDB {

// helper macro
#define BIGINT_COMPARE_FUNC(OP)                                             \
    switch (rhs.GetTypeId()) {                                              \
    case TypeId::TINYINT:                                                   \
        return GetCmpBool(lhs.value_.bigint_ OP rhs.value_.tinyint_);       \
    case TypeId::SMALLINT:                                                  \
        return GetCmpBool(lhs.value_.bigint_ OP rhs.value_.smallint_);      \
    case TypeId::INTEGER:                                                   \
        return GetCmpBool(lhs.value_.bigint_ OP rhs.value_.integer_);       \
    case TypeId::BIGINT:                                                    \
        return GetCmpBool(lhs.value_.bigint_ OP rhs.value_.bigint_);        \
    case TypeId::DECIMAL:                                                   \
        return GetCmpBool(lhs.value_.bigint_ OP rhs.value_.decimal_);       \
    case TypeId::VARCHAR: {                                                 \
        auto val = rhs.CastAs(TypeId::BIGINT);                              \
        return GetCmpBool(lhs.value_.bigint_ OP val.value_.bigint_);        \
    }                                                                       \
    default:                                                                \
        break;                                                              \
    }

// method stands for our helper function defined in parent class
#define BIGINT_MODIFY_FUNC(METHOD, OP)                                                  \
    switch (rhs.GetTypeId()) {                                                          \
    case TypeId::TINYINT:                                                               \
        return METHOD<int64_t, int8_t>(lhs, rhs);                                       \
    case TypeId::SMALLINT:                                                              \
        return METHOD<int64_t, int16_t>(lhs, rhs);                                      \
    case TypeId::INTEGER:                                                               \
        return METHOD<int64_t, int32_t>(lhs, rhs);                                      \
    case TypeId::BIGINT:                                                                \
        return METHOD<int64_t, int64_t>(lhs, rhs);                                      \
    case TypeId::DECIMAL:                                                               \
        return Value(TypeId::DECIMAL, lhs.value_.bigint_ OP rhs.value_.decimal_);       \
    case TypeId::VARCHAR: {                                                             \
        auto val = rhs.CastAs(TypeId::BIGINT);                                          \
        return METHOD<int64_t, int64_t>(lhs, val);                                      \
    }                                                                                   \
    default:                                                                            \
        break;                                                                          \
    }

bool BigintType::IsZero(const Value &val) const {
    return val.value_.bigint_ == 0;
}

Value BigintType::Add(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));
    
    if (lhs.IsNull() || rhs.IsNull()) {
        // should we return null directly
        return lhs.OperateNull(rhs);
    }

    BIGINT_MODIFY_FUNC(AddValue, +);

    UNREACHABLE("type error");
}

Value BigintType::Subtract(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    BIGINT_MODIFY_FUNC(SubtractValue, -);

    UNREACHABLE("type error");
}

Value BigintType::Multiply(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    BIGINT_MODIFY_FUNC(MultiplyValue, *);

    UNREACHABLE("type error");
}

Value BigintType::Divide(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    if (rhs.IsZero()) {
        THROW_DIVIDE_BY_ZERO_EXCEPTION("Division by zero");
    }

    BIGINT_MODIFY_FUNC(DivideValue, /);

    UNREACHABLE("type error");
}

Value BigintType::Modulo(const Value &lhs, const Value &rhs) const {
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
        return ModuloValue<int64_t, int8_t>(lhs, rhs);                                  
    case TypeId::SMALLINT:                                                        
        return ModuloValue<int64_t, int16_t>(lhs, rhs);                                 
    case TypeId::INTEGER:                                                         
        return ModuloValue<int64_t, int32_t>(lhs, rhs);                                 
    case TypeId::BIGINT:                                                          
        return ModuloValue<int64_t, int64_t>(lhs, rhs);                                 
    case TypeId::DECIMAL:                                                         
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.bigint_, rhs.value_.decimal_));
    case TypeId::VARCHAR: {                                                       
        auto val = rhs.CastAs(TypeId::BIGINT);                                   
        return ModuloValue<int64_t, int64_t>(lhs, val);                                  
    }                                                                             
    default:                                                                      
        break;                                                                    
    }

    UNREACHABLE("type error");
}

Value BigintType::Sqrt(const Value &val) const {
    assert(val.CheckInteger());
    
    if (val.IsNull()) {
        // should we return decimal or integer?
        return Value(TypeId::DECIMAL, TINYDB_DECIMAL_NULL);
    }
    if (val.value_.integer_ < 0) {
        THROW_DECIMAL_EXCEPTION("trying to apply sqrt on negative number");
    }

    return Value(TypeId::DECIMAL, std::sqrt(val.value_.bigint_));
}

Value BigintType::OperateNull(const Value &lhs, const Value &rhs) const {
    return Type::Null(lhs.GetTypeId());
}

CmpBool BigintType::CompareEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    BIGINT_COMPARE_FUNC(==);

    UNREACHABLE("type error");
}

CmpBool BigintType::CompareNotEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    BIGINT_COMPARE_FUNC(!=);

    UNREACHABLE("type error");
}

CmpBool BigintType::CompareLessThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    BIGINT_COMPARE_FUNC(<);

    UNREACHABLE("type error");
}

CmpBool BigintType::CompareLessThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    BIGINT_COMPARE_FUNC(<=);

    UNREACHABLE("type error");
}

CmpBool BigintType::CompareGreaterThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    BIGINT_COMPARE_FUNC(>);

    UNREACHABLE("type error");
}

CmpBool BigintType::CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    BIGINT_COMPARE_FUNC(>=);

    UNREACHABLE("type error");
}

std::string BigintType::ToString(const Value &val) const {
    assert(val.CheckInteger());
    if (val.IsNull()) {
        return "bigint_null";
    }

    return std::to_string(val.value_.bigint_);
}

// serialize/deserialize for storage

void BigintType::SerializeTo(const Value &val, char *storage) const {
    *reinterpret_cast<int32_t *>(storage) = val.value_.bigint_;
}

Value BigintType::DeserializeFrom(const char *storage) const {
    auto val = *reinterpret_cast<const int64_t *>(storage);
    return Value(type_id_, val);
}

Value BigintType::Copy(const Value &val) const {
    assert(val.CheckInteger());
    return Value(TypeId::BIGINT, val.value_.bigint_);
}

Value BigintType::CastAs(const Value &val, const TypeId type_id) const {
    if (val.IsNull()) {
        return Type::Null(type_id);
    }

    // check whether we can cast to type_id;
    assert(IsCoercableTo(type_id));

    switch (type_id) {
    case TypeId::TINYINT:
        return Value(type_id, static_cast<int8_t>(val.value_.bigint_));
    case TypeId::SMALLINT:
        return Value(type_id, static_cast<int16_t>(val.value_.bigint_));
    case TypeId::INTEGER:
        return Value(type_id, static_cast<int32_t>(val.value_.bigint_));
    case TypeId::BIGINT:
        return Copy(val);
    case TypeId::DECIMAL:
        return Value(type_id, static_cast<double>(val.value_.bigint_));
    case TypeId::VARCHAR:
        return Value(type_id, val.ToString());
    default:
        break;
    }

    UNREACHABLE("cannot cast bigint to " + Type::TypeToString(type_id));
}


}