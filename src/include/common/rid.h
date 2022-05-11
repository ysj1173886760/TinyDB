/**
 * @file rid.h
 * @author sheep
 * @brief record id/tuple id
 * @version 0.1
 * @date 2022-05-07
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef RID_H
#define RID_H

#include "common/config.h"

#include <sstream>

namespace TinyDB {

/**
 * @brief 
 * RID. i.e. record id, or tuple id. You can regard it as a pointer in TinyDB that points
 * to a tuple. higher 32 bit is page id, lower 32 bit is slot id.
 */
class RID {
public:
    // create default RID. i.e. invalid rid
    RID(): page_id_(INVALID_PAGE_ID), slot_id_(0) {}

    /**
     * @brief Construct a new RID object
     * 
     * @param page_id id of page
     * @param slot_id id of slot. i.e. slot number
     */
    RID(page_id_t page_id, uint32_t slot_id): page_id_(page_id), slot_id_(slot_id) {}

    /**
     * @brief Construct a new RID object using pointer
     * higher 32 bit is page id, lower 32 bit is slot id
     * @param rid 
     */
    explicit RID(int64_t rid): 
        page_id_(static_cast<page_id_t>(rid >> 32)), 
        slot_id_(static_cast<uint32_t>(rid)) {}
    
    RID &operator=(const RID &rid) = default;
    RID(const RID &rid) = default;
    
    /**
     * @brief 
     * return the pointer form RID
     * @return int64_t 
     */
    inline int64_t Get() const {
        return (static_cast<int64_t>(page_id_) << 32) | slot_id_;
    }

    inline page_id_t GetPageId() const {
        return page_id_;
    }

    inline uint32_t GetSlotId() const {
        return slot_id_;
    }

    inline void Set(page_id_t page_id, uint32_t slot_id) {
        page_id_ = page_id;
        slot_id_ = slot_id;
    }

    inline std::string ToString() const {
        std::ostringstream os;
        os << "page_id: " << page_id_;
        os << " slot_id: " << slot_id_ << "\n";

        return os.str();
    }

    friend std::ostream &operator<<(std::ostream &os, const RID &rid) {
        os << rid.ToString();
        return os;
    }

    bool operator==(const RID &rhs) const {
        return page_id_ == rhs.page_id_ &&
               slot_id_ == rhs.slot_id_;
    }

    bool operator!=(const RID &rhs) const {
        return !(*this == rhs);
    }

    void Swap(RID &rhs) {
        std::swap(page_id_, rhs.page_id_);
        std::swap(slot_id_, rhs.slot_id_);
    }

private:
    page_id_t page_id_{INVALID_PAGE_ID};
    uint32_t slot_id_{0};
};

}

namespace std {
// specifiy the hash function for RID
template<>
struct hash<TinyDB::RID> {
    size_t operator()(const TinyDB::RID &rid) {
        return hash<int64_t>()(rid.Get());
    }
};

}

#endif