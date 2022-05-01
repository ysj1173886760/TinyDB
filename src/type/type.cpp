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

}

#endif