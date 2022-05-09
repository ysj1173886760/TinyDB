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

    // copy constructor
    Tuple(const Tuple &other);
    Tuple &operator=(Tuple other);

    void Swap(Tuple &rhs) {
        std::swap(rhs.data_, data_);
        std::swap(rhs.size_, size_);
        rid_.Swap(rhs.rid_);
    }

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

    /**
     * @brief Get the tuple length, including varlen object
     * @return uint32_t 
     */
    inline uint32_t GetSize() const {
        return size_;
    }

    inline void SetRID(const RID &rid) {
        rid_ = rid;
    }

    // get the value of a specified column
    Value GetValue(const Schema *schema, uint32_t column_idx) const;

    /**
     * @brief 
     * generate a key tuple given schemas and attributes
     * @param schema schema of current tuple
     * @param key_schema schema of returned tuple
     * @param key_attrs indices of the columns of old schema that will constitute new schema
     * @return Tuple 
     */
    Tuple KeyFromTuple(const Schema *schema, const Schema *key_schema, const std::vector<uint32_t> &key_attrs);

    /**
     * @brief 
     * generate a tuple by giving base schema and target schema. And we will generate key_attrs list ourself
     * @param schema 
     * @param key_schema 
     * @return Tuple 
     */
    Tuple KeyFromTuple(const Schema *schema, const Schema *key_schema);

    // Is the column value null?
    inline bool IsNull(const Schema *schema, uint32_t column_idx) const {
        Value value = GetValue(schema, column_idx);
        return value.IsNull();
    }

    std::string ToString(const Schema *schema) const;

    // serialize tuple data with size
    void SerializeToWithSize(char *storage) const;

    // deserialize tuple data with size
    static Tuple DeserializeFromWithSize(const char *storage);

    /**
     * @brief 
     * serialize tuple data without size. 
     * this serialization method is not self-contained, 
     * thus we need other metadata to store the size.
     * @param storage buffer contains tuple data
     */
    void SerializeTo(char *storage) const;

    /**
     * @brief 
     * deserialize tuple without size, same as above
     * @param storage buffer contains tuple data
     * @param size tuple size
     * @return Tuple 
     */
    static Tuple DeserializeFrom(const char *storage, uint32_t size);

    /**
     * @brief 
     * deserialize the tuple and store the new data inplace.
     * i.e. replace current data in tuple with the one in storage.
     * we will clear the previous data buffer.
     * storage contains the size.
     * @param storage data buffer
     */

    void DeserializeFromInplaceWithSize(const char *storage);
    /**
     * @brief 
     * deserialize the tuple and store the new data inplace.
     * i.e. replace current data in tuple with the one in storage.
     * we will clear the previous data buffer.
     * storage didn't contain size, so we need to pass in manually
     * @param storage data buffer
     * @param size tuple size
     */
    void DeserializeFromInplace(const char *storage, uint32_t size);

private:
    // get the starting storage address of specific column
    const char *GetDataPtr(const Schema *schema, uint32_t column_idx) const;

    // default is invalid rid
    RID rid_{};

    // total size of this tuple
    uint32_t size_{0};

    // payload
    char *data_{nullptr};
};

}

#endif