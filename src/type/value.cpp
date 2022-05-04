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
    len_ = other.len_;
    value_ = other.value_;
    switch (type_id_) {
    case TypeId::VARCHAR:
        if (len_ == TINYDB_VALUE_NULL) {
            value_.varlen_ = nullptr;
        } else {
            value_.varlen_ = new char[len_];
            memcpy(value_.varlen_, other.value_.varlen_, len_);
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
        len_ = (value_.boolean_ == TINYDB_BOOLEAN_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    case TypeId::TINYINT:
        value_.tinyint_ = i;
        len_ = (value_.tinyint_ == TINYDB_INT8_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        THROW_INCOMPATIBLE_TYPE_EXCEPTION("Invalid Type for one-byte value constructor")
    }
}

// small int
Value::Value(TypeId type_id, int16_t i): Value(type_id) {
    switch (type_id) {
    case TypeId::SMALLINT:
        value_.smallint_ = i;
        len_ = (value_.smallint_ == TINYDB_INT16_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        THROW_INCOMPATIBLE_TYPE_EXCEPTION("Invalid Type for two-byte value constructor")
    }
}

// integer
Value::Value(TypeId type_id, int32_t i): Value(type_id) {
    switch (type_id) {
    case TypeId::INTEGER:
        value_.integer_ = i;
        len_ = (value_.integer_ == TINYDB_INT32_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        THROW_INCOMPATIBLE_TYPE_EXCEPTION("Invalid Type for four-byte value constructor")
    }
}

// bigint
Value::Value(TypeId type_id, int64_t i): Value(type_id) {
    switch (type_id) {
    case TypeId::BIGINT:
        value_.bigint_ = i;
        len_ = (value_.bigint_ == TINYDB_INT64_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        THROW_INCOMPATIBLE_TYPE_EXCEPTION("Invalid Type for eight-byte value constructor")
    }
}

// timestamp
Value::Value(TypeId type_id, uint64_t i): Value(type_id) {
    switch (type_id) {
    case TypeId::TIMESTAMP:
        value_.timestamp_ = i;
        len_ = (value_.timestamp_ == TINYDB_TIMESTAMP_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        THROW_INCOMPATIBLE_TYPE_EXCEPTION("Invalid Type for timestamp value constructor")
    }
}

// decimal
Value::Value(TypeId type_id, double d) : Value(type_id) {
    switch (type_id) {
    case TypeId::DECIMAL:
        value_.decimal_ = d;
        len_ = (value_.decimal_ == TINYDB_DECIMAL_NULL ? TINYDB_VALUE_NULL : 0);
        break;
    default:
        THROW_INCOMPATIBLE_TYPE_EXCEPTION("Invalid Type for decimal value constructor")
    }
}

// varchar
Value::Value(TypeId type_id, const char *data, uint32_t len): Value(type_id) {
    switch (type_id) {
    case TypeId::VARCHAR:
        if (data == nullptr) {
            value_.varlen_ = nullptr;
            len_ = TINYDB_VALUE_NULL;
        } else {
            // we don't put data into additional buffer
            // we manage it directly
            // maybe we should use object pool to gain better performance?
            value_.varlen_ = new char[len];
            len_ = len;
            memcpy(value_.varlen_, data, len);
        }
        break;
    default:
        THROW_INCOMPATIBLE_TYPE_EXCEPTION("Invalid Type for varchar value constructor");
    }
}

Value::Value(TypeId type_id, const std::string &data): Value(type_id) {
    switch(type_id) {
    case TypeId::VARCHAR:
        len_ = data.length();
        // TODO: figure out whether do we need + 1
        // maybe we need? since we may need cstring lib
        value_.varlen_ = new char[len_];
        memcpy(value_.varlen_, data.c_str(), len_);
        break;
    default:
        THROW_INCOMPATIBLE_TYPE_EXCEPTION("Invalid Type for varchar value constructor");
    }
}

Value::~Value() {
    switch (type_id_) {
    case TypeId::VARCHAR:
        delete[] value_.varlen_;
        break;
    default:
        break;
    }
}

bool Value::CheckComparable(const Value &rhs) const {
    switch (type_id_) {
    case TypeId::BOOLEAN:
        return (rhs.GetTypeId() == TypeId::BOOLEAN ||
                rhs.GetTypeId() == TypeId::VARCHAR);
    case TypeId::TINYINT:
    case TypeId::SMALLINT:
    case TypeId::INTEGER:
    case TypeId::BIGINT:
    case TypeId::DECIMAL:
        // i'm missing rust
        switch (rhs.GetTypeId()) {
        case TypeId::TINYINT:
        case TypeId::SMALLINT:
        case TypeId::INTEGER:
        case TypeId::BIGINT:
        case TypeId::DECIMAL:
        case TypeId::VARCHAR:
            return true;
        default:
            break;
        }
        break;
    case TypeId::VARCHAR:
        // Anything can be cast to a string
        return true;
    default:
        break;
    }
    return false;
}

bool Value::CheckInteger() const {
    switch (type_id_) {
    case TypeId::TINYINT:
    case TypeId::SMALLINT:
    case TypeId::INTEGER:
    case TypeId::BIGINT:
        return true;
    default:
        return false;
    }
}

}

#endif