/**
 * @file limits.h
 * @author sheep
 * @brief defines the special values in our type subsystem
 * @version 0.1
 * @date 2022-05-01
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef LIMITS_H
#define LIMITS_H

#include <climits>
#include <limits>
#include <cstdint>

namespace TinyDB {

// like andy said, we will use special value to represent null field
// this requires more boundary check when dealing with user input
// another possible approach is to use bitmap to indicate null field
// but that would introduce additional overhead and dealing with delta
// storage would be tricky
// I would chose to use same approach like bustub. i.e. use special values to represent null
// since i'm borrowing code from bustub
// I might came back and change this when i've the global view design space in databases

// NULL values

// used for internal invalid values
static constexpr uint32_t TINYDB_VALUE_NULL = UINT_MAX;
static constexpr int8_t TINYDB_INT8_NULL = SCHAR_MIN;
static constexpr int16_t TINYDB_INT16_NULL = SHRT_MIN;
static constexpr int32_t TINYDB_INT32_NULL = INT_MIN;
static constexpr int64_t TINYDB_INT64_NULL = LLONG_MIN;
static constexpr int8_t TINYDB_BOOLEAN_NULL = SCHAR_MIN;

}

#endif