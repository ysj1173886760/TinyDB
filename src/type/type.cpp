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

#include "type/type.h"
#include "common/exception.h"
#include "type/value.h"

#include <string>

namespace TinyDB {

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

    throw Exception(ExceptionType::UNKNOWN_TYPE, "GetTypeSize::Unknown type");
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

// when sub-class didn't implement those functions, it will fallback to 
// base class and throw an exception
CmpBool Type::CompareEquals(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement compare equals");
}

CmpBool Type::CompareNotEquals(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement compare not equals");
}

CmpBool Type::CompareLessThan(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement compare less than");
}

CmpBool Type::CompareLessThanEquals(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement compare less than equals");
}

CmpBool Type::CompareGreaterThan(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement compare greater than");
}

CmpBool Type::CompareGreaterThanEquals(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement compare greater than equals");
}

Value Type::Add(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement add");
}

Value Type::Subtract(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement subtract");
}

Value Type::Multiply(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement multiply");
}

Value Type::Divide(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement divide");
}

Value Type::Modulo(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement modulo");
}

Value Type::Min(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement min");
}
 
Value Type::Max(const Value &left, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement max");
}
 
Value Type::Sqrt(const Value &val) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement sqrt");
}
 
Value Type::OperateNull(const Value &val, const Value &right) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement operate null");
}

bool Type::IsZero(const Value &val) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement is zero");
}

bool Type::IsInlined(const Value &val) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement is inlined");
}

std::string Type::ToString(const Value &val) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement to string");
}

std::string Type::SerializeToString(const Value &val) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement serialize to string");
}

Value Type::DeserializeFromString(const std::string &data) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement deserialize from string");
}

void Type::SerializeTo(const Value &val, char *storage) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement serialize to");
}

Value Type::DeserializeFrom(const char *storage) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement deserialize from");
}

Value Type::Copy(const Value &val) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement copy");
}

Value Type::CastAs(const Value &val, TypeId type_id) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement cast as");
}

const char *Type::GetData(const Value &val) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement get data");
}

uint32_t Type::GetLength(const Value &val) const {
    throw NotImplementedException(Type::TypeToString(type_id_) + " doesn't implement get length");
}

}

#endif