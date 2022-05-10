/**
 * @file varlen_type.cpp
 * @author sheep
 * @brief 
 * @version 0.1
 * @date 2022-05-06
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "common/macros.h"
#include "common/logger.h"
#include "common/exception.h"
#include "type/varlen_type.h"

#include <cstring>
#include <assert.h>

namespace TinyDB {

// since we are using non-null-terminated string, we will use memcmp
// to compare string directly
// return value is the same as strcmp
// < 0 means the first character that doesn't match has a lower value in str1 than str2
// = 0 means the contents of both strings are equal
// > 0 means the first character that doesn't match has a greater value in str1 that str2
static inline int CompareStrings(const char *str1, int len1, const char *str2, int len2) {
    assert(str1 != nullptr);
    assert(len1 >= 0);
    assert(str2 != nullptr);
    assert(len2 >= 0);

    int ret = memcmp(str1, str2, static_cast<size_t>(std::min(len1, len2)));
    if (ret == 0 && len1 != len2) {
        ret = len1 - len2;
    }
    return ret;
}

#define VARLEN_COMPARE_FUNC(OP)                                     \
    const char *str1 = lhs.GetData();                               \
    uint32_t len1 = lhs.GetLength();                                \
    const char *str2;                                               \
    uint32_t len2;                                                  \
    if (rhs.GetTypeId() == TypeId::VARCHAR) {                       \
        str2 = rhs.GetData();                                       \
        len2 = rhs.GetLength();                                     \
    } else {                                                        \
        auto r_value = rhs.CastAs(TypeId::VARCHAR);                 \
        str2 = r_value.GetData();                                   \
        len2 = r_value.GetLength();                                 \
    }                                                               \
    return GetCmpBool(CompareStrings(str1, len1, str2, len2) OP 0);

CmpBool VarlenType::CompareEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    // TODO: length of varchar is limited, we should check it

    VARLEN_COMPARE_FUNC(==);
}

CmpBool VarlenType::CompareNotEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    VARLEN_COMPARE_FUNC(!=);
}

CmpBool VarlenType::CompareLessThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    VARLEN_COMPARE_FUNC(<);
}

CmpBool VarlenType::CompareLessThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    VARLEN_COMPARE_FUNC(<=);
}

CmpBool VarlenType::CompareGreaterThan(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    VARLEN_COMPARE_FUNC(>);
}

CmpBool VarlenType::CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    VARLEN_COMPARE_FUNC(>=);
}

Value VarlenType::Min(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    if (lhs.IsNull() || rhs.IsNull()) {
        return Type::Null(TypeId::VARCHAR);
    }
    if (lhs.CompareLessThan(rhs) == CmpBool::CmpTrue) {
        return lhs.Copy();
    }
    return rhs.Copy();
}

Value VarlenType::Max(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    if (lhs.IsNull() || rhs.IsNull()) {
        return Type::Null(TypeId::VARCHAR);
    }
    if (lhs.CompareGreaterThan(rhs) == CmpBool::CmpTrue) {
        return lhs.Copy();
    }
    return rhs.Copy();
}

std::string VarlenType::ToString(const Value &val) const {
    if (val.IsNull()) {
        return "varlen_null";
    }

    uint32_t len = val.GetLength();
    if (len == 0) {
        return "";
    }
    return std::string(val.GetData(), len);
}

void VarlenType::SerializeTo(const Value &val, char *storage) const {
    if (val.IsNull()) {
        memcpy(storage, &val.len_, sizeof(uint32_t));
        return;
    }
    uint32_t len = val.GetLength();
    memcpy(storage, &len, sizeof(uint32_t));
    memcpy(storage + sizeof(uint32_t), val.value_.varlen_, len);
}

Value VarlenType::DeserializeFrom(const char *storage) const {
    uint32_t len = *reinterpret_cast<const uint32_t *> (storage);
    if (len == TINYDB_VALUE_NULL) {
        return Type::Null(TypeId::VARCHAR);
    }
    return Value(TypeId::VARCHAR, storage + sizeof(uint32_t), len);
}

Value VarlenType::Copy(const Value &val) const {
    // copy constructor will help us
    return Value(val);
}

// TODO: handle failure
// implement a FA ourself that can handle this conversion
Value VarlenType::CastAs(const Value &value, const TypeId type_id) const {
    assert(IsCoercableTo(type_id));
    std::string str;
    switch (type_id) {
    case TypeId::BOOLEAN: {
        str = value.ToString();
        // to lower
        std::transform(str.begin(), str.end(), str.begin(), ::tolower);
        if (str == "true" || str == "1" || str == "t") {
            return Value(type_id, static_cast<int8_t>(1));
        }
        if (str == "false" || str == "0" || str == "f") {
            return Value(type_id, static_cast<int8_t>(0));
        }
        // should we throw exception or just return null?
        LOG_ERROR("failed to cast %s to boolean", str.c_str());
        return Type::Null(type_id);
    }
    case TypeId::TINYINT: {
        str = value.ToString();
        int8_t tinyint = 0;
        tinyint = static_cast<int8_t>(stoi(str));
        return Value(type_id, tinyint);
    }
    case TypeId::SMALLINT: {
        str = value.ToString();
        int16_t smallint = 0;
        smallint = static_cast<int16_t>(stoi(str));
        return Value(type_id, smallint);
    }
    case TypeId::INTEGER: {
        str = value.ToString();
        int32_t integer = 0;
        integer = static_cast<int32_t>(stoi(str));
        return Value(type_id, integer);
    }
    case TypeId::BIGINT: {
        str = value.ToString();
        int64_t bigint = 0;
        bigint = static_cast<int64_t>(stol(str));
        return Value(type_id, bigint);
    }
    case TypeId::DECIMAL: {
        str = value.ToString();
        double decimal = 0;
        decimal = static_cast<double>(stod(str));
        return Value(type_id, decimal);
    }
    case TypeId::TIMESTAMP: {
        THROW_NOT_IMPLEMENTED_EXCEPTION("casting from str to timestamp is not implemented");
        
    }
    case TypeId::VARCHAR: {
        return value.Copy();
    }
    default:
        break;
    }
    UNREACHABLE("logic error");
}

}