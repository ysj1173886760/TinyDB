/**
 * @file value.cpp
 * @author sheep
 * @brief value module
 * @version 0.1
 * @date 2022-05-01
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef VALUE_CPP
#define VALUE_CPP

#include "common/exception.h"
#include "type/value.h"

#include <string>
#include <cstring>

namespace TinyDB {

Value::Value(const Value &other) {
    type_id_ = other.type_id_;
    size_ = other.size_;
    value_ = other.value_;
    switch (type_id_) {
    case TypeId::VARCHAR:
        if (size_.len_ == TINYDB_VALUE_NULL) {
            value_.varlen_ = nullptr;
        } else {
            value_.varlen_ = new char[size_.len_];
            memcpy(value_.varlen_, other.value_.varlen_, size_.len_);
        }
        break;
    default:
        value_ = other.value_;
    }
}

// exception safe since std::swap won't throw exception
Value &Value::operator=(Value other) {
    Swap(*this, other);
    return *this;
}

// boolean and tinyint
Value::Value(TypeId type_id, int8_t i): Value(type_id) {
    switch (type_id) {
    case TypeId::BOOLEAN:
        value_.boolean_ = i;
        size_.len_ = (value_.boolean_ == TINYDB_BOOLEAN_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    case TypeId::TINYINT:
        value_.tinyint_ = i;
        size_.len_ = (value_.tinyint_ == TINYDB_INT8_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for one-byte value constructor");
    }
}

// small int
Value::Value(TypeId type_id, int16_t i): Value(type_id) {
    switch (type_id) {
    case TypeId::SMALLINT:
        value_.smallint_ = i;
        size_.len_ = (value_.smallint_ == TINYDB_INT16_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for two-byte value constructor");
    }
}

// integer
Value::Value(TypeId type_id, int32_t i): Value(type_id) {
    switch (type_id) {
    case TypeId::INTEGER:
        value_.integer_ = i;
        size_.len_ = (value_.integer_ == TINYDB_INT32_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for four-byte value constructor");
    }
}

// bigint
Value::Value(TypeId type_id, int64_t i): Value(type_id) {
    switch (type_id) {
    case TypeId::INTEGER:
        value_.bigint_ = i;
        size_.len_ = (value_.bigint_ == TINYDB_INT64_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for eight-byte value constructor");
    }
}

}

#endif