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
#include "type/type.h"

#include <cstdint>
#include <string>
#include <iostream>

namespace TinyDB {

// comments from bustub
// A value is an abstract class that represents a view over SQL data stored in
// some materialized state. All values have a type and comparison functions, but
// subclasses implement other type-specific functionality.
class Value {
public:
    explicit Value(const TypeId type_id)
        : type_id_(type_id) {
        // initialize value as null
        size_.len_ = TINYDB_VALUE_NULL;
    }
    // constructors

    // bool and tinyint
    Value(TypeId type_id, int8_t i);
    // Decimal
    Value(TypeId type_id, double d);
    Value(TypeId type_id, float f);
    // Small int
    Value(TypeId type_id, int16_t i);
    // Integer
    Value(TypeId type_id, int32_t i);
    // Bigint
    Value(TypeId type_id, int64_t i);
    // Timestamp
    Value(TypeId type_id, uint64_t i);
    // Varchar
    Value(TypeId type_id, const char *data, uint32_t len);
    Value(TypeId type_id, const std::string &data);

    // default
    Value(): Value(TypeId::INVALID) {}
    Value(const Value &other);
    Value &operator=(Value other);
    ~Value();

    friend void Swap(Value &first, Value &second) {
        std::swap(first.value_, second.value_);
        std::swap(first.size_, second.size_);
        std::swap(first.type_id_, second.type_id_);
    }

    // get metadata and raw data

    inline TypeId GetTypeId() const { return type_id_; }

    inline uint32_t GetLength() const { return Type::GetInstance(type_id_)->GetLength(*this); }

    inline const char *GetData() const { return Type::GetInstance(type_id_)->GetData(*this); }

    inline Value CastAs(const TypeId type_id) const {
        return Type::GetInstance(type_id_)->CastAs(*this, type_id);
    }

    // comparison functions

    inline CmpBool CompareEquals(const Value &rhs) const {
        return Type::GetInstance(type_id_)->CompareEquals(*this, rhs);
    }
    inline CmpBool CompareNotEquals(const Value &rhs) const {
        return Type::GetInstance(type_id_)->CompareNotEquals(*this, rhs);
    }
    inline CmpBool CompareLessThan(const Value &rhs) const {
        return Type::GetInstance(type_id_)->CompareLessThan(*this, rhs);
    }
    inline CmpBool CompareLessThanEquals(const Value &rhs) const {
        return Type::GetInstance(type_id_)->CompareLessThanEquals(*this, rhs);
    }
    inline CmpBool CompareGreaterThan(const Value &rhs) const {
        return Type::GetInstance(type_id_)->CompareGreaterThan(*this, rhs);
    }
    inline CmpBool CompareGreaterThanEquals(const Value &rhs) const {
        return Type::GetInstance(type_id_)->CompareGreaterThanEquals(*this, rhs);
    }

    // mathematical functions

    inline Value Add(const Value &rhs) const {
        return Type::GetInstance(type_id_)->Add(*this, rhs);
    }
    inline Value Subtract(const Value &rhs) const {
        return Type::GetInstance(type_id_)->Subtract(*this, rhs);
    }
    inline Value Multiply(const Value &rhs) const {
        return Type::GetInstance(type_id_)->Multiply(*this, rhs);
    }
    inline Value Divide(const Value &rhs) const {
        return Type::GetInstance(type_id_)->Divide(*this, rhs);
    }
    inline Value Modulo(const Value &rhs) const {
        return Type::GetInstance(type_id_)->Modulo(*this, rhs);
    }
    inline Value Min(const Value &rhs) const {
        return Type::GetInstance(type_id_)->Min(*this, rhs);
    }
    inline Value Max(const Value &rhs) const {
        return Type::GetInstance(type_id_)->Max(*this, rhs);
    }
    inline Value Sqrt() const {
        return Type::GetInstance(type_id_)->Sqrt(*this);
    }
    inline Value OperateNull(const Value &rhs) const {
        return Type::GetInstance(type_id_)->OperateNull(*this, rhs);
    }
    inline bool IsZero() const {
        return Type::GetInstance(type_id_)->IsZero(*this);
    }
    inline bool IsNull() const {
        // size_.len_ = TINYDB_VALUE_NULL means this value is null
        return size_.len_ == TINYDB_VALUE_NULL;
    }

    // other special functions
    // comments from bustub
    // Serialize this value into the given storage space. The inlined parameter
    // indicates whether we are allowed to inline this value into the storage
    // space, or whether we must store only a reference to this value. If inlined
    // is false, we may use the provided data pool to allocate space for this
    // value, storing a reference into the allocated pool space in the storage.
    inline void SerializeTo(char *storage) const {
        Type::GetInstance(type_id_)->SerializeTo(*this, storage);
    }

    inline std::string SerializeToString() const {
        return Type::GetInstance(type_id_)->SerializeToString(*this);
    }

    // deserialize a value of given type from the given storage space
    // use static function here, because you can't call member function before create this object
    inline static Value DeserializeFrom(const char *storage, const TypeId type_id) {
        return Type::GetInstance(type_id)->DeserializeFrom(storage);
    }

    inline static Value DeserializeFromString(const std::string &data, const TypeId type_id) {
        return Type::GetInstance(type_id)->DeserializeFromString(data);
    }

    // Return a string version of this value
    inline std::string ToString() const {
        return Type::GetInstance(type_id_)->ToString(*this);
    }
    // Create a copy of this value
    inline Value Copy() const {
        return Type::GetInstance(type_id_)->Copy(*this);
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