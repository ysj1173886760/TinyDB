/**
 * @file exception.h
 * @author sheep
 * @brief custom exceptions from bustub
 * @version 0.1
 * @date 2022-05-01
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef EXCEPTION_H
#define EXCEPTION_H

#include <stdexcept>
#include <iostream>
#include <string>
#include <regex>

namespace TinyDB {

enum class ExceptionType {
    // Invalid type
    INVALID = 0,
    // value out of range
    OUT_OF_RANGE = 1,
    // casting error
    CONVERSION = 2,
    // unknown type in type subsystem
    UNKNOWN_TYPE = 3,
    // decimal related errors
    DECIMAL = 4,
    // type mismatch
    MISMATCH_TYPE = 5,
    // division by 0
    DIVIDE_BY_ZERO = 6,
    // incompatible type
    INCOMPATIBLE_TYPE = 7,
    // out of memory error
    OUT_OF_MEMORY = 8,
    // method not implemented
    NOT_IMPLEMENTED = 9,
    // io related error
    IO = 10,
    // unreachable, this should belong to logic error
    UNREACHABLE = 11,
};

// we don't use try catch, so any exception will cause a crash
class Exception : public std::runtime_error {
public:
    explicit Exception(const std::string &message)
        : std::runtime_error(message), type_(ExceptionType::INVALID) {
        std::string exception_message = "Message :: " + message + "\n";
        std::cerr << exception_message;
    }

    Exception(ExceptionType type, const std::string &message, const char *file, int line)
        : std::runtime_error(message), type_(type) {

        std::string loc = std::string(file) + "; line " + std::to_string(line);
        std::string exception_message = 
            "******* Exception Type :: " + ExceptionTypeToString(type) + "\n" +
            "******* Message :: " + message + "\n" +
            "******* @ Location: " + loc + "\n";
        std::cerr << exception_message;
    }

    std::string ExceptionTypeToString(ExceptionType type) {
        switch (type) {
            case ExceptionType::INVALID:
                return "Invalid";
            case ExceptionType::OUT_OF_RANGE:
                return "Out of Range";
            case ExceptionType::CONVERSION:
                return "Conversion";
            case ExceptionType::UNKNOWN_TYPE:
                return "Unknown Type";
            case ExceptionType::DECIMAL:
                return "Decimal";
            case ExceptionType::MISMATCH_TYPE:
                return "Mismatch Type";
            case ExceptionType::DIVIDE_BY_ZERO:
                return "Divide by Zero";
            case ExceptionType::INCOMPATIBLE_TYPE:
                return "Incompatible Type";
            case ExceptionType::OUT_OF_MEMORY:
                return "Out of Memory";
            case ExceptionType::NOT_IMPLEMENTED:
                return "Not Implemented";
            case ExceptionType::IO:
                return "IO";
            case ExceptionType::UNREACHABLE:
                return "Unreachable";
            default:
                return "Unknown Exception Type";
        }
    }
private:
    ExceptionType type_;
};

#define THROW_NOT_IMPLEMENT_EXCEPTION(msg)  \
    throw Exception(ExceptionType::NOT_IMPLEMENTED, msg, __FILE__, __LINE__);

#define THROW_OUT_OF_RANGE_EXCEPTION(msg)  \
    throw Exception(ExceptionType::OUT_OF_RANGE, msg, __FILE__, __LINE__);

#define THROW_CONVERSION_EXCEPTION(msg)  \
    throw Exception(ExceptionType::CONVERSION, msg, __FILE__, __LINE__);

#define THROW_UNKNOWN_TYPE_EXCEPTION(msg)  \
    throw Exception(ExceptionType::UNKNOWN_TYPE, msg, __FILE__, __LINE__);

#define THROW_DECIMAL_EXCEPTION(msg)  \
    throw Exception(ExceptionType::DECIMAL, msg, __FILE__, __LINE__);

#define THROW_MISMATCH_TYPE_EXCEPTION(msg)  \
    throw Exception(ExceptionType::MISMATCH_TYPE, msg, __FILE__, __LINE__);

#define THROW_DIVIDE_BY_ZERO_EXCEPTION(msg)  \
    throw Exception(ExceptionType::DIVIDE_BY_ZERO, msg, __FILE__, __LINE__);

#define THROW_INCOMPATIBLE_TYPE_EXCEPTION(msg)  \
    throw Exception(ExceptionType::INCOMPATIBLE_TYPE, msg, __FILE__, __LINE__);

#define THROW_OUT_OF_MEMORY_EXCEPTION(msg)  \
    throw Exception(ExceptionType::OUT_OF_MEMORY, msg, __FILE__, __LINE__);

#define THROW_IO_EXCEPTION(msg)  \
    throw Exception(ExceptionType::IO, msg, __FILE__, __LINE__);

#define THROW_UNREACHABLE_EXCEPTION(msg)  \
    throw Exception(ExceptionType::UNREACHABLE, msg, __FILE__, __LINE__);

}

#endif