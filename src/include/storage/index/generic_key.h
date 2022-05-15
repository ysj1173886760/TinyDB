/**
 * @file generic_key.h
 * @author sheep
 * @brief generic key
 * @version 0.1
 * @date 2022-05-12
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef GENERIC_KEY_H
#define GENERIC_KEY_H

#include "storage/table/tuple.h"
#include "type/value.h"

#include <cassert>

namespace TinyDB {

/**
 * @brief 
 * brief from bustub:
 * Generic key is used for indexing with opaque data.
 * This key type uses an fixed length array to hold data for indexing purposes,
 * the actual size of which is specified and instantiated with a template argument
 * @tparam KeySize 
 */
template <size_t KeySize>
class GenericKey {
public:
    inline void SetFromKey(const Tuple &tuple) {
        // initialize to all zero
        memset(data_, 0, KeySize);
        assert(tuple.GetLength() <= KeySize);
        memcpy(data_, tuple.GetData(), tuple.GetLength());
    }

    inline Value ToValue(Schema *schema, uint32_t column_idx) const {
        const auto &col = schema->GetColumn(column_idx);
        if (col.IsInlined()) {
            return Value::DeserializeFrom(data_ + col.GetOffset(), col.GetType());
        } else {
            uint32_t offset = *reinterpret_cast<const uint32_t *> (data_ + col.GetOffset());
            return Value::DeserializeFrom(data_ + offset, col.GetType());
        }
    }

    inline const char *ToBytes() const {
        return data_;
    }

    char data_[KeySize];
};

template<size_t KeySize> 
class GenericComparator {
public:
    explicit GenericComparator(Schema *key_schema) : key_schema_(key_schema) {}
    GenericComparator(const GenericComparator &other) : key_schema_(other.key_schema_) {}

    inline int operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
        uint32_t column_cnt = key_schema_->GetColumnCount();
        for (uint32_t i = 0; i < column_cnt; i++) {
            Value lhs_value = lhs.ToValue(key_schema_, i);
            Value rhs_value = rhs.ToValue(key_schema_, i);

            if (lhs_value.CompareLessThan(rhs_value) == CmpBool::CmpTrue) {
                return -1;
            }

            if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue) {
                return 1;
            }

        }
        // TODO: should we check null?
        return 0;
    }

private:
    Schema *key_schema_;
};

}

#endif