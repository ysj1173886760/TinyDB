/**
 * @file tuple.h
 * @author sheep
 * @brief tuple
 * @version 0.1
 * @date 2022-05-07
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TUPLE_H
#define TUPLE_H

#include "common/rid.h"
#include "catalog/schema.h"
#include "type/value.h"

namespace TinyDB {

/**
 * @brief 
 * description of single tuple that stays in memory
 * Tuple format:
 * | FIXED-SIZE VALUE or VARIED-SIZE OFFSET | PAYLOAD OF VARIED-SIZE TYPE
 * i.e. for every column, either it contains the corresponding fixed-size value which can be 
 * retrieved based on column-offset in schema, or it contains the offset of varied-size type, and 
 * the corresponding payload is placed at the end of the tuple
 */
class Tuple {
public:
    // default tuple, which doesn't have any specific data nor the information
    Tuple() = default;

    // create tuple from values and corresponding schema
    Tuple(std::vector<Value> values, const Schema *schema);

    ~Tuple();

    // helper functions

    inline RID GetRID() const {
        return rid_;
    }
    
    // TODO: should we return const char *?
    inline char *GetData() const {
        return data_;
    }

    /**
     * @brief Get the tuple length, including varlen object
     * @return uint32_t 
     */
    inline uint32_t GetLength() const {
        return size_;
    }

    inline bool IsAllocated() const {
        return allocated_;
    }

private:
    // is tuple allocated?
    // maybe we can get this attribute by checking whether data pointer is null
    bool allocated_{false};
    
    // default is invalid rid
    RID rid_{};

    // total size of this tuple
    uint32_t size_{0};

    // payload
    char *data_{nullptr};
};

}

#endif