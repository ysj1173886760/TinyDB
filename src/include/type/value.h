/**
 * @file value.h
 * @author sheep
 * @brief value for type subsystem
 * @version 0.1
 * @date 2022-05-01
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef VALUE_H
#define VALUE_H

#include "type/type_id.h"
#include "type/limits.h"

#include <cstdint>

namespace TinyDB {

// comments from bustub
// A value is an abstract class that represents a view over SQL data stored in
// some materialized state. All values have a type and comparison functions, but
// subclasses implement other type-specific functionality.
class Value {
public:
    explicit Value(const TypeId type_id)
        : type_id_(type_id) {
        // initialize with all one
        size_.len_ = TINYDB_VALUE_NULL;
    }
private:
    // data it's self
    union Val {
        int8_t boolean_;
        int8_t tinyint_;
        int16_t smallint_;
        int32_t integer_;
        int64_t bigint_;
        double decimal_;
        uint64_t timestamp_;
        char *varlen_;
        const char *const_varlen_;
    } value_;
    
    // size or elem type for varlen type
    union {
        uint32_t len_;
        TypeId elem_type_id_;
    } size_;

    TypeId type_id_;
};

}

#endif