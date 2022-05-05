/**
 * @file type.cpp
 * @author sheep
 * @brief type subsystem
 * @version 0.1
 * @date 2022-05-01
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TYPE_CPP
#define TYPE_CPP

#include "common/exception.h"
#include "common/macros.h"
#include "type/type.h"
#include "type/value.h"
#include "type/tinyint_type.h"
#include "type/smallint_type.h"
#include "type/boolean_type.h"

#include <string>

namespace TinyDB {

// initialize static member in cpp to prevent multiple defintions
// this is not necessary, but we prefer to put it in source file instead of header
// to avoid endless recurrent header reference
Type *Type::k_types_[] = {
    new Type(TypeId::INVALID),
    // place holder for bool
    new BooleanType(),
    new TinyintType(),
    new SmallintType(),
    // integer
    new Type(TypeId::INVALID),
    // bigint
    new Type(TypeId::INVALID),
    // decimal
    new Type(TypeId::INVALID),
    // varchar
    new Type(TypeId::INVALID),
    // timestamp
    new Type(TypeId::INVALID),
};

uint64_t Type::GetTypeSize(const TypeId type_id) {
    switch (type_id) {
    case TypeId::BOOLEAN:
        // fall though
    case TypeId::TINYINT:
        return 1;
    case TypeId::SMALLINT:
        return 2;
    case TypeId::INTEGER:
        return 4;
    case TypeId::BIGINT:
        // fall though
    case TypeId::DECIMAL:
        // fall though
    case TypeId::TIMESTAMP:
        return 8;
    case TypeId::VARCHAR:
        // should use GetLength
        return 0;
    default:
        break;
    }

    THROW_UNKNOWN_TYPE_EXCEPTION("GetTypeSize::Unknown type");
}

std::string Type::TypeToString(const TypeId type_id) {
    switch (type_id) {
    case TypeId::INVALID:
        return "INVALID";
    case TypeId::BOOLEAN:
        return "BOOLEAN";
    case TypeId::TINYINT:
        return "TINYINT";
    case TypeId::SMALLINT:
        return "SMALLINT";
    case TypeId::INTEGER:
        return "INTEGER";
    case TypeId::BIGINT:
        return "BIGINT";
    case TypeId::DECIMAL:
        return "DECIMAL";
    case TypeId::TIMESTAMP:
        return "TIMESTAMP";
    case TypeId::VARCHAR:
        return "VARCHAR";
    default:
        return "INVALID";
    }
}

Value Type::Null(const TypeId type_id) {
    switch (type_id) {
    case TypeId::BOOLEAN:
        return Value(type_id, TINYDB_BOOLEAN_NULL);
    case TypeId::TINYINT:
        return Value(type_id, TINYDB_INT8_NULL);
    case TypeId::SMALLINT:
        return Value(type_id, TINYDB_INT16_NULL);
    case TypeId::INTEGER:
        return Value(type_id, TINYDB_INT32_NULL);
    case TypeId::BIGINT:
        return Value(type_id, TINYDB_INT64_NULL);
    case TypeId::DECIMAL:
        return Value(type_id, TINYDB_DECIMAL_NULL);
    case TypeId::VARCHAR:
        return Value(type_id, nullptr, 0);
    case TypeId::TIMESTAMP:
        return Value(type_id, TINYDB_TIMESTAMP_NULL);
    default:
        break;
    }

    UNREACHABLE("invalid type");
}

bool Type::IsCoercableFrom(const TypeId type_id) const {
    if (type_id == INVALID || type_id_ == INVALID) {
        return false;
    }
    switch (type_id_) {
    case VARCHAR:
        // everyone can coerce to varchar
        return true;

    // avoid comparsion between boolean and numeric type
    case BOOLEAN:
        return (type_id == VARCHAR || type_id == BOOLEAN);

    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
        switch (type_id) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case VARCHAR:
            return true;
        default:
            return false;
        }

    case TIMESTAMP:
        return (type_id == VARCHAR || type_id == TIMESTAMP);
    
    default:
        return false;
    }
}

bool Type::IsTrue(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(TypeToString(type_id_) + " doesn't implement IsTrue");
}

bool Type::IsFalse(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(TypeToString(type_id_) + " doesn't implement IsFalse");
}

// when sub-class didn't implement those functions, it will fallback to 
// base class and throw an exception
CmpBool Type::CompareEquals(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement compare equals");
    
}

CmpBool Type::CompareNotEquals(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement compare not equals");
}

CmpBool Type::CompareLessThan(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement compare less than");
}

CmpBool Type::CompareLessThanEquals(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement compare less than equals");
}

CmpBool Type::CompareGreaterThan(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement compare greater than");
}

CmpBool Type::CompareGreaterThanEquals(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement compare greater than equals");
}

Value Type::Add(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement add");
}

Value Type::Subtract(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement subtract");
}

Value Type::Multiply(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement multiply");
}

Value Type::Divide(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement divide");
}

Value Type::Modulo(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement modulo");
}

Value Type::Min(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement min");
}
 
Value Type::Max(const Value &left, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement max");
}
 
Value Type::Sqrt(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement sqrt");
}
 
Value Type::OperateNull(const Value &val, const Value &right) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement operate null");
}

bool Type::IsZero(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement is zero");
}

bool Type::IsInlined(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement is inlined");
}

std::string Type::ToString(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement to string");
}

std::string Type::SerializeToString(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement serialize to string");
}

Value Type::DeserializeFromString(const std::string &data) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement deserialize from string");
}

void Type::SerializeTo(const Value &val, char *storage) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement serialize to");
}

Value Type::DeserializeFrom(const char *storage) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement deserialize from");
}

Value Type::Copy(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement copy");
}

Value Type::CastAs(const Value &val, TypeId type_id) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement cast as");
}

const char *Type::GetData(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement get data");
}

uint32_t Type::GetLength(const Value &val) const {
    THROW_NOT_IMPLEMENT_EXCEPTION(Type::TypeToString(type_id_) + " doesn't implement get length");
}

}

#endif