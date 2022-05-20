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

Tuple::Tuple(const Tuple &other) : rid_(other.rid_),
                                   size_(other.size_) {
    if (other.data_ != nullptr) {
        data_ = new char[size_];
        memcpy(data_, other.data_, size_);
    }
    // otherwise, data will be null
}

Tuple &Tuple::operator=(Tuple other) {
    other.Swap(*this);
    return *this;
}

Tuple::~Tuple() {
    delete[] data_;
}

const char *Tuple::GetDataPtr(const Schema *schema, const uint32_t column_idx) const {
    assert(schema != nullptr);
    assert(data_ != nullptr);
    const auto &col = schema->GetColumn(column_idx);
    
    if (col.IsInlined()) {
        return (data_ + col.GetOffset());
    }

    uint32_t offset = *reinterpret_cast<const uint32_t *> (data_ + col.GetOffset());

    // if it's null, then we return the inlined address directly
    if (offset == TINYDB_VALUE_NULL) {
        return (data_ + col.GetOffset());
    }

    return data_ + offset;
}

Value Tuple::GetValue(const Schema *schema, const uint32_t column_idx) const {
    const char *data_ptr = GetDataPtr(schema, column_idx);
    const TypeId column_type = schema->GetColumn(column_idx).GetType();

    return Value::DeserializeFrom(data_ptr, column_type);
}

Tuple Tuple::KeyFromTuple(const Schema *schema, const Schema *key_schema, const std::vector<uint32_t> &key_attrs) const {
    std::vector<Value> values;
    values.reserve(key_attrs.size());
    for (uint32_t idx : key_attrs) {
        values.emplace_back(this->GetValue(schema, idx));
    }
    
    return Tuple(values, key_schema);
}

Tuple Tuple::KeyFromTuple(const Schema *schema, const Schema *key_schema) const {
    auto key_attrs = key_schema->GenerateKeyAttrs(schema);
    return KeyFromTuple(schema, key_schema, key_attrs);
}

void Tuple::SerializeToWithSize(char *storage) const {
    // do we need to serialize size_ here?
    // i think we can retrieve all of the metadata from tuple indirectly though fixed-length data field
    // because we can get the last varlen offset though schema and read the length of that varlen type
    // then we get the whole tuple length
    // anyway, i think 4 byte is not a big deal here. for the sake of simplicity, i will store tuple size here
    memcpy(storage, &size_, sizeof(uint32_t));
    memcpy(storage + sizeof(uint32_t), data_, size_);
}

Tuple Tuple::DeserializeFromWithSize(const char *storage) {
    auto tuple = Tuple();
    uint32_t size = *reinterpret_cast<const uint32_t *>(storage);
    tuple.size_ = size;

    tuple.data_ = new char[tuple.size_];
    memcpy(tuple.data_, storage + sizeof(uint32_t), tuple.size_);

    return tuple;
}

void Tuple::SerializeTo(char *storage) const {
    memcpy(storage, data_, size_);
}

Tuple Tuple::DeserializeFrom(const char *storage, uint32_t size) {
    auto tuple = Tuple();
    tuple.size_ = size;

    tuple.data_ = new char[tuple.size_];
    memcpy(tuple.data_, storage, tuple.size_);

    return tuple;
}

void Tuple::DeserializeFromInplace(const char *storage, uint32_t size) {
    // this implementation should be exception safe
    // also with good code reuse
    auto new_tuple = Tuple::DeserializeFrom(storage, size);
    this->Swap(new_tuple);
}

void Tuple::DeserializeFromInplaceWithSize(const char *storage) {
    auto new_tuple = Tuple::DeserializeFromWithSize(storage);
    this->Swap(new_tuple);
}

std::string Tuple::ToString(const Schema *schema) const {
    std::stringstream os;
    int len = static_cast<int>(schema->GetColumnCount());
    
    os << "(";
    for (int i = 0; i < len - 1; i++) {
        auto value = GetValue(schema, i);
        os << value.ToString() << ", ";
    }
    os << GetValue(schema, len - 1).ToString() << ") Tuple size is " << size_;

    return os.str();
}

}
