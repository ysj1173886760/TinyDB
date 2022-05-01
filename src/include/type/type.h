/**
 * @file type.h
 * @author sheep
 * @brief type is magic
 * @version 0.1
 * @date 2022-05-01
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TYPE_H
#define TYPE_H

#include "type/type_id.h"

#include <stddef.h>
#include <cstdint>
#include <string>

namespace TinyDB {

/**
 * @brief 
 * TLDR: use runtime polymorphism to mimic type
 * because the type in database is dynamic, so we can't 
 * simply using generic to create table or index. Instead, 
 * we need a indirection layer to mimic the runtime generic
 * instead of using compiler to do this.
 * basic idea is we can parse types from user command into some
 * internal type_id, then use factory to create corresponding type
 * and simulate the related operations.
 */
class Type {
public:
    explicit Type(TypeId type_id): type_id_(type_id) {}
    virtual ~Type() = default;

    // Get the size of this data type in bytes
    static uint64_t GetTypeSize(TypeId type_id);

    // for debug
    static std::string ToString(TypeId type_id);

    inline TypeId GetTypeId() const { return type_id_; }


protected:
    TypeId type_id_;

};

}

#endif