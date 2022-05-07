/**
 * @file tuple.h
 * @author sheep
 * @brief implementation of tuple
 * @version 0.1
 * @date 2022-05-07
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/table/tuple.h"

#include <assert.h>
#include <cstring>

namespace TinyDB {

Tuple::Tuple(std::vector<Value> values, const Schema *schema) {
    assert(values.size() == schema->GetColumnCount());

    // calculate the size of tuple
    // for varlen type, if it's null, the size would be 4
    // otherwise it's 4 + length. i.e. size + data

    // get the fixed length
    size_ = schema->GetLength();
    // calc the total length of varlen type
    for (uint32_t i : schema->GetUninlinedColumns()) {
        // we don't store data for null type
        if (values[i].IsNull()) {
            continue;
        }
        // size + data
        size_ += values[i].GetSerializedLength();
    }

    // allocate memory
    data_ = new char[size_];
    memset(data_, 0, size_);

    // serialize values into the tuple
    // store the offset of varlen type
    uint32_t offset = schema->GetLength();
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        const auto &col = schema->GetColumn(i);
        if (col.IsInlined()) {
            // serialize inlined type directly
            values[i].SerializeTo(data_ + col.GetOffset());
        } else {
            if (values[i].IsNull()) {
                // if value is null, then we serialize the null value directly
                *reinterpret_cast<uint32_t *>(data_ + col.GetOffset()) = TINYDB_VALUE_NULL;
            } else {
                // serialize the offset of varlen type
                *reinterpret_cast<uint32_t *>(data_ + col.GetOffset()) = offset;
                values[i].SerializeTo(data_ + offset);
                offset += values[i].GetSerializedLength();
            }
        }
    }
}

}
