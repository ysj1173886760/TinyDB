/**
 * @file decimal_type.cpp
 * @author sheep
 * @brief decimal type implementation
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/decimal_type.h"
#include "common/macros.h"
#include "common/exception.h"

#include <string>
#include <assert.h>

namespace TinyDB {

static constexpr double eps = 1e-10;

// helper macro
#define DECIMAL_COMPARE_FUNC(OP)                                            \
    switch (rhs.GetTypeId()) {                                              \
    case TypeId::TINYINT:                                                   \
        return GetCmpBool(lhs.value_.decimal_ OP rhs.value_.tinyint_);      \
    case TypeId::SMALLINT:                                                  \
        return GetCmpBool(lhs.value_.decimal_ OP rhs.value_.smallint_);     \
    case TypeId::INTEGER:                                                   \
        return GetCmpBool(lhs.value_.decimal_ OP rhs.value_.integer_);      \
    case TypeId::BIGINT:                                                    \
        return GetCmpBool(lhs.value_.decimal_ OP rhs.value_.bigint_);       \
    case TypeId::DECIMAL:                                                   \
        return GetCmpBool(lhs.value_.decimal_ OP rhs.value_.decimal_);      \
    case TypeId::VARCHAR: {                                                 \
        auto val = rhs.CastAs(TypeId::DECIMAL);                             \
        return GetCmpBool(lhs.value_.decimal_ OP val.value_.decimal_);      \
    }                                                                       \
    default:                                                                \
        break;                                                              \
    }

// method stands for our helper function defined in parent class
// we will care about overflow problem in integer type. but we don't do that in decimal type
// so we will do arithmetic operation here directly
#define DECIMAL_MODIFY_FUNC(OP)                                                         \
    switch (rhs.GetTypeId()) {                                                          \
    case TypeId::TINYINT:                                                               \
        return Value(TypeId::DECIMAL, lhs.value_.decimal_ OP rhs.value_.tinyint_);      \
    case TypeId::SMALLINT:                                                              \
        return Value(TypeId::DECIMAL, lhs.value_.decimal_ OP rhs.value_.smallint_);     \
    case TypeId::INTEGER:                                                               \
        return Value(TypeId::DECIMAL, lhs.value_.decimal_ OP rhs.value_.integer_);      \
    case TypeId::BIGINT:                                                                \
        return Value(TypeId::DECIMAL, lhs.value_.decimal_ OP rhs.value_.bigint_);       \
    case TypeId::DECIMAL:                                                               \
        return Value(TypeId::DECIMAL, lhs.value_.decimal_ OP rhs.value_.decimal_);      \
    case TypeId::VARCHAR: {                                                             \
        auto val = rhs.CastAs(TypeId::DECIMAL);                                         \
        return Value(TypeId::DECIMAL, lhs.value_.decimal_ OP rhs.value_.decimal_);      \
    }                                                                                   \
    default:                                                                            \
        break;                                                                          \
    }

bool DecimalType::IsZero(const Value &val) const {
    return val.value_.decimal_ == 0;
}

Value DecimalType::Add(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    
    if (lhs.IsNull() || rhs.IsNull()) {
        // should we return null directly
        return lhs.OperateNull(rhs);
    }

    DECIMAL_MODIFY_FUNC(+);

    UNREACHABLE("type error");
}

Value DecimalType::Subtract(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    DECIMAL_MODIFY_FUNC(-);

    UNREACHABLE("type error");
}

Value DecimalType::Multiply(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    DECIMAL_MODIFY_FUNC(*);

    UNREACHABLE("type error");
}

Value DecimalType::Divide(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return lhs.OperateNull(rhs);
    }

    if (rhs.IsZero()) {
        THROW_DIVIDE_BY_ZERO_EXCEPTION("Division by zero");
    }

    DECIMAL_MODIFY_FUNC(/);

    UNREACHABLE("type error");
}

Value DecimalType::Modulo(const Value &lhs, const Value &rhs) const {
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
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.decimal_, static_cast<double>(rhs.value_.tinyint_)));
    case TypeId::SMALLINT:                                                        
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.decimal_, static_cast<double>(rhs.value_.smallint_)));
    case TypeId::INTEGER:                                                         
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.decimal_, static_cast<double>(rhs.value_.integer_)));
    case TypeId::BIGINT:                                                          
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.decimal_, static_cast<double>(rhs.value_.bigint_)));
    case TypeId::DECIMAL:                                                         
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.decimal_, rhs.value_.decimal_));
    case TypeId::VARCHAR: {                                                       
        auto val = rhs.CastAs(TypeId::DECIMAL);                                   
        return Value(TypeId::DECIMAL, ValMod(lhs.value_.decimal_, rhs.value_.decimal_));
    }                                                                             
    default:                                                                      
        break;                                                                    
    }

    UNREACHABLE("type error");
}

Value DecimalType::Sqrt(const Value &val) const {
    if (val.IsNull()) {
        return Value(TypeId::DECIMAL, TINYDB_DECIMAL_NULL);
    }
    if (val.value_.integer_ < 0) {
        THROW_DECIMAL_EXCEPTION("trying to apply sqrt on negative number");
    }

    return Value(TypeId::DECIMAL, std::sqrt(val.value_.decimal_));
}

Value DecimalType::OperateNull(const Value &lhs, const Value &rhs) const {
    return Type::Null(lhs.GetTypeId());
}

CmpBool DecimalType::CompareEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    switch (rhs.GetTypeId()) {                                         
    case TypeId::TINYINT:                                              
        return GetCmpBool(abs(lhs.value_.decimal_ - rhs.value_.tinyint_) < eps); 
    case TypeId::SMALLINT:                                             
        return GetCmpBool(abs(lhs.value_.decimal_ - rhs.value_.smallint_) < eps);
    case TypeId::INTEGER:                                              
        return GetCmpBool(abs(lhs.value_.decimal_ - rhs.value_.integer_) < eps); 
    case TypeId::BIGINT:                                               
        return GetCmpBool(abs(lhs.value_.decimal_ - rhs.value_.bigint_) < eps);  
    case TypeId::DECIMAL:                                              
        return GetCmpBool(abs(lhs.value_.decimal_ - rhs.value_.decimal_) < eps); 
    case TypeId::VARCHAR: {                                            
        auto val = rhs.CastAs(TypeId::DECIMAL);                        
        return GetCmpBool(abs(lhs.value_.decimal_ - val.value_.decimal_) < eps); 
    }                                                                  
    default:                                                           
        break;                                                         
    }

    UNREACHABLE("type error");
}

CmpBool DecimalType::CompareNotEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }
    return CompareEquals(lhs, rhs) == CmpBool::CmpTrue ? CmpBool::CmpFalse : CmpBool::CmpTrue;
}

CmpBool DecimalType::CompareLessThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    DECIMAL_COMPARE_FUNC(<);

    UNREACHABLE("type error");
}

CmpBool DecimalType::CompareLessThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    DECIMAL_COMPARE_FUNC(<=);

    UNREACHABLE("type error");
}

CmpBool DecimalType::CompareGreaterThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    DECIMAL_COMPARE_FUNC(>);

    UNREACHABLE("type error");
}

CmpBool DecimalType::CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));

    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    DECIMAL_COMPARE_FUNC(>=);

    UNREACHABLE("type error");
}

std::string DecimalType::ToString(const Value &val) const {
    if (val.IsNull()) {
        return "decimal_null";
    }

    return std::to_string(val.value_.decimal_);
}

// serialize/deserialize for storage
// note that serialize/deserialize has nothing to do with string
// we will serialize those into byte array, instead of some meaningful string

void DecimalType::SerializeTo(const Value &val, char *storage) const {
    *reinterpret_cast<double *>(storage) = val.value_.decimal_;
}

Value DecimalType::DeserializeFrom(const char *storage) const {
    auto val = *reinterpret_cast<const double *>(storage);
    return Value(type_id_, val);
}

Value DecimalType::Copy(const Value &val) const {
    return Value(TypeId::DECIMAL, val.value_.decimal_);
}

Value DecimalType::CastAs(const Value &val, const TypeId type_id) const {
    if (val.IsNull()) {
        return Type::Null(type_id);
    }

    // check whether we can cast to type_id;
    assert(IsCoercableTo(type_id));

    switch (type_id) {
    case TypeId::TINYINT:
        return Value(type_id, static_cast<int8_t>(val.value_.decimal_));
    case TypeId::SMALLINT:
        return Value(type_id, static_cast<int16_t>(val.value_.decimal_));
    case TypeId::INTEGER:
        return Value(type_id, static_cast<int32_t>(val.value_.decimal_));
    case TypeId::BIGINT:
        return Value(type_id, static_cast<int64_t>(val.value_.decimal_));
    case TypeId::DECIMAL:
        return Copy(val);
    case TypeId::VARCHAR:
        return Value(type_id, val.ToString());
    default:
        break;
    }

    UNREACHABLE("cannot cast decimal to " + Type::TypeToString(type_id));
}


}