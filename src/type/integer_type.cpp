/**
 * @file integer_type.cpp
 * @author sheep
 * @brief integer type implementation
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/integer_type.h"
#include "common/macros.h"

#include <string>
#include <assert.h>

namespace TinyDB {

// helper macro
#define INTEGER_COMPARE_FUNC(OP)                                            \
    switch (rhs.GetTypeId()) {                                              \
    case TypeId::TINYINT:                                                   \
        return GetCmpBool(lhs.value_.integer_ OP rhs.value_.tinyint_);      \
    case TypeId::SMALLINT:                                                  \
        return GetCmpBool(lhs.value_.integer_ OP rhs.value_.smallint_);     \
    case TypeId::INTEGER:                                                   \
        return GetCmpBool(lhs.value_.integer_ OP rhs.value_.integer_);      \
    case TypeId::BIGINT:                                                    \
        return GetCmpBool(lhs.value_.integer_ OP rhs.value_.bigint_);       \
    case TypeId::DECIMAL:                                                   \
        return GetCmpBool(lhs.value_.integer_ OP rhs.value_.decimal_);      \
    case TypeId::VARCHAR: {                                                 \
        auto val = rhs.CastAs(TypeId::INTEGER);                             \
        return GetCmpBool(lhs.value_.integer_ OP val.value_.integer_);      \
    }                                                                       \
    default:                                                                \
        break;                                                              \
    }

// method stands for our helper function defined in parent class
#define INTEGER_MODIFY_FUNC(METHOD, OP)                                                 \
    switch (rhs.GetTypeId()) {                                                          \
    case TypeId::TINYINT:                                                               \
        return METHOD<int32_t, int8_t>(lhs, rhs);                                       \
    case TypeId::SMALLINT:                                                              \
        return METHOD<int32_t, int16_t>(lhs, rhs);                                      \
    case TypeId::INTEGER:                                                               \
        return METHOD<int32_t, int32_t>(lhs, rhs);                                      \
    case TypeId::BIGINT:                                                                \
        return METHOD<int32_t, int64_t>(lhs, rhs);                                      \
    case TypeId::DECIMAL:                                                               \
        return Value(TypeId::DECIMAL, lhs.value_.integer_ OP rhs.value_.decimal_);      \
    case TypeId::VARCHAR: {                                                             \
        auto val = rhs.CastAs(TypeId::INTEGER);                                         \
        return METHOD<int32_t, int32_t>(lhs, val);                                      \
    }                                                                                   \
    default:                                                                            \
        break;                                                                          \
    }

bool IntegerType::IsZero(const Value &val) const {
    return val.value_.integer_ == 0;
}

Value IntegerType::Add(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));
    
    if (lhs.IsNull() || rhs.IsNull()) {
        // should we return null directly
        return lhs.OperateNull(rhs);
    }

    INTEGER_MODIFY_FUNC(AddValue, +);

    UNREACHABLE("type error");
}

Value IntegerType::Subtract(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    INTEGER_MODIFY_FUNC(SubtractValue, -);

    UNREACHABLE("type error");
}

Value IntegerType::Multiply(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    INTEGER_MODIFY_FUNC(MultiplyValue, *);

    UNREACHABLE("type error");
}

Value IntegerType::Divide(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    if (rhs.IsZero()) {
        THROW_DIVIDE_BY_ZERO_EXCEPTION("Division by zero");
    }

    INTEGER_MODIFY_FUNC(DivideValue, /);

    UNREACHABLE("type error");
}

Value IntegerType::Modulo(const Value &lhs, const Value &rhs) const {
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
        return ModuloValue<int32_t, int8_t>(lhs, rhs);                                  
    case TypeId::SMALLINT:                                                        
        return ModuloValue<int32_t, int16_t>(lhs, rhs);                                 
    case TypeId::INTEGER:                                                         
        return ModuloValue<int32_t, int32_t>(lhs, rhs);                                 
    case TypeId::BIGINT:                                                          
        return ModuloValue<int32_t, int64_t>(lhs, rhs);                                 
    case TypeId::DECIMAL:                                                         
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.integer_, rhs.value_.decimal_));
    case TypeId::VARCHAR: {                                                       
        auto val = rhs.CastAs(TypeId::INTEGER);                                   
        return ModuloValue<int32_t, int32_t>(lhs, val);                                  
    }                                                                             
    default:                                                                      
        break;                                                                    
    }

    UNREACHABLE("type error");
}

Value IntegerType::Sqrt(const Value &val) const {
    assert(val.CheckInteger());
    
    if (val.IsNull()) {
        // should we return decimal or integer?
        return Value(TypeId::DECIMAL, TINYDB_DECIMAL_NULL);
    }
    if (val.value_.integer_ < 0) {
        THROW_DECIMAL_EXCEPTION("trying to apply sqrt on negative number");
    }

    return Value(TypeId::DECIMAL, std::sqrt(val.value_.integer_));
}

// since null value will propagate, so i think type is not matter here
Value IntegerType::OperateNull(const Value &lhs, const Value &rhs) const {
    return Type::Null(lhs.GetTypeId());
}

CmpBool IntegerType::CompareEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    INTEGER_COMPARE_FUNC(==);

    UNREACHABLE("type error");
}

CmpBool IntegerType::CompareNotEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    INTEGER_COMPARE_FUNC(!=);

    UNREACHABLE("type error");
}

CmpBool IntegerType::CompareLessThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    INTEGER_COMPARE_FUNC(<);

    UNREACHABLE("type error");
}

CmpBool IntegerType::CompareLessThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    INTEGER_COMPARE_FUNC(<=);

    UNREACHABLE("type error");
}

CmpBool IntegerType::CompareGreaterThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    INTEGER_COMPARE_FUNC(>);

    UNREACHABLE("type error");
}

CmpBool IntegerType::CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckInteger());
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    INTEGER_COMPARE_FUNC(>=);

    UNREACHABLE("type error");
}

std::string IntegerType::ToString(const Value &val) const {
    assert(val.CheckInteger());
    if (val.IsNull()) {
        return "integer_null";
    }

    return std::to_string(val.value_.integer_);
}

// serialize/deserialize for storage
// note that serialize/deserialize has nothing to do with string
// we will serialize those into byte array, instead of some meaningful string

void IntegerType::SerializeTo(const Value &val, char *storage) const {
    *reinterpret_cast<int32_t *>(storage) = val.value_.integer_;
}

Value IntegerType::DeserializeFrom(const char *storage) const {
    auto val = *reinterpret_cast<const int32_t *>(storage);
    return Value(type_id_, val);
}

Value IntegerType::Copy(const Value &val) const {
    assert(val.CheckInteger());
    return Value(TypeId::INTEGER, val.value_.integer_);
}

Value IntegerType::CastAs(const Value &val, const TypeId type_id) const {
    if (val.IsNull()) {
        return Type::Null(type_id);
    }

    // check whether we can cast to type_id;
    assert(IsCoercableTo(type_id));

    switch (type_id) {
    case TypeId::TINYINT:
        return Value(type_id, static_cast<int8_t>(val.value_.integer_));
    case TypeId::SMALLINT:
        return Value(type_id, static_cast<int16_t>(val.value_.integer_));
    case TypeId::INTEGER:
        return Copy(val);
    case TypeId::BIGINT:
        return Value(type_id, static_cast<int64_t>(val.value_.integer_));
    case TypeId::DECIMAL:
        return Value(type_id, static_cast<double>(val.value_.integer_));
    case TypeId::VARCHAR:
        return Value(type_id, val.ToString());
    default:
        break;
    }

    UNREACHABLE("cannot cast integer to " + Type::TypeToString(type_id));
}


}