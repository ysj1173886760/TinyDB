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

class Value;

// didn't use simple true and false 
// since we may have some type that is not comparable
enum class CmpBool {
    CmpFalse = 0,
    CmpTrue = 1,
    CmpNull = 2
};

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
    static uint64_t GetTypeSize(const TypeId type_id);

    // for debug
    static std::string TypeToString(const TypeId type_id);

    // return the null value corresponding to the type
    static Value Null(const TypeId type_id);

    inline TypeId GetTypeId() const { return type_id_; }

    inline static Type *GetInstance(const TypeId type_id) { return k_types_[type_id]; }

    // virtual functions
    // define the generic operations, we will apply those operations
    // on values based on type id
    // A common way to do this is construct the specific type based on
    // type id and apply the corresponding operations

    // comments below is from bustub
    // Comparison functions
    //
    // NOTE:
    // We could get away with only CompareLessThan() being purely virtual, since
    // the remaining comparison functions can derive their logic from
    // CompareLessThan(). For example:
    //
    //    CompareEquals(o) = !CompareLessThan(o) && !o.CompareLessThan(this)
    //    CompareNotEquals(o) = !CompareEquals(o)
    //    CompareLessThanEquals(o) = CompareLessThan(o) || CompareEquals(o)
    //    CompareGreaterThan(o) = !CompareLessThanEquals(o)
    //    ... etc. ...
    //
    // We don't do this for two reasons:
    // (1) The redundant calls to CompareLessThan() may be a performance problem,
    //     and since Value is a core component of the execution engine, we want to
    //     make it as performant as possible.
    // (2) Keep the interface consistent by making all functions purely virtual.

    // =
    virtual CmpBool CompareEquals(const Value &lhs, const Value &rhs) const;
    // !=
    virtual CmpBool CompareNotEquals(const Value &lhs, const Value &rhs) const;
    // <
    virtual CmpBool CompareLessThan(const Value &lhs, const Value &rhs) const;
    // <=
    virtual CmpBool CompareLessThanEquals(const Value &lhs, const Value &rhs) const;
    // >
    virtual CmpBool CompareGreaterThan(const Value &lhs, const Value &rhs) const;
    // >=
    virtual CmpBool CompareGreaterThanEquals(const Value &lhs, const Value &rhs) const;

    // mathematical functions
    // TODO: figure out should we define inplace operations?
    // this is interesting, could we extend this to buld something more?
    // e.g. more types, more operators

    // +
    virtual Value Add(const Value &lhs, const Value &rhs) const;
    // -
    virtual Value Subtract(const Value &lhs, const Value &rhs) const;
    // *
    virtual Value Multiply(const Value &lhs, const Value &rhs) const;
    // /
    virtual Value Divide(const Value &lhs, const Value &rhs) const;
    // %
    virtual Value Modulo(const Value &lhs, const Value &rhs) const;
    // min
    virtual Value Min(const Value &lhs, const Value &rhs) const;
    // max
    virtual Value Max(const Value &lhs, const Value &rhs) const;
    // sqrt
    virtual Value Sqrt(const Value &val) const;
    // operation when some one is null
    // since any value operate with null will get null, so we will return null value directly
    // and i think type is not matter here, so we will return null value corresponding to lhs type
    virtual Value OperateNull(const Value &lhs, const Value &right) const;
    // not sure whether empty string is zero element for string type
    virtual bool IsZero(const Value &val) const;

    // special operations

    // Is the data inlined into this classes storage space, or must it be accesses
    // through an indirection layer / pointer ?
    virtual bool IsInlined(const Value &val) const;

    // return a stringified version of this value
    // i.e. serialize it to string
    virtual std::string ToString(const Value &val) const;

    // !!! deprecated !!!
    // another approach is serialize to string, then we can store data to some kvs
    // RID -> SerializedValue
    // thus we can exploit this api to use KVS as storage engine
    // serialize this value to string. i wonder is this dupliated with ToString?
    virtual std::string SerializeToString(const Value &val) const;

    // !!! deprecated !!!
    // deserialize value from string
    virtual Value DeserializeFromString(const std::string &data) const;
    
    // Serialize this value into the given storage space
    virtual void SerializeTo(const Value &val, char *storage) const;

    // Deserialize a value from storage
    virtual Value DeserializeFrom(const char *storage) const;

    // Create a copy of this value
    virtual Value Copy(const Value &val) const;

    // cast value type
    virtual Value CastAs(const Value &val, TypeId type_id) const;

    // variable length data related

    // Access the raw variable length data
    virtual const char *GetData(const Value &val) const;

    // Get the length of variable length data
    virtual uint32_t GetLength(const Value &val) const;

protected:
    // Type id
    TypeId type_id_;
    // instances
    static Type *k_types_[20];
};

}

#endif