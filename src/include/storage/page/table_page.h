/**
 * @file table_page.h
 * @author sheep
 * @brief page that contains the real data. table_page is responsible to
 * store tuple into the disk. so it belongs to the storage layer.
 * STATEMENT BELOW IS WRONG. INTERMEDIATE LAYER IS SOMEWHERE ELSE.
 * this is the intermediate layer between transaction layer and storage layer.
 * i.e. it's responsible to interpret data to tuple based on schema
 * @version 0.1
 * @date 2022-05-08
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TABLE_PAGE_H
#define TABLE_PAGE_H

#include "common/rid.h"
#include "storage/page/page.h"
#include "storage/table/tuple.h"

namespace TinyDB {

// highest bit in 32bit integer
static constexpr uint32_t DELETE_MASK = (1U << (8 * sizeof(uint32_t) - 1));

/**
 * @brief 
 * format from bustub
 * Slotted page format:
 * ---------------------------------------------------------
 * | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 * ---------------------------------------------------------
 *                               ^
 *                               free space pointer
 * Header format(size in bytes)
 * ----------------------------------------------------------------------------
 * | PageId(4) | LSN(4) | PrevPageId(4) | NextPageId(4) | FreeSpacePointer(4) |
 * ----------------------------------------------------------------------------
 * ---------------------------------------------------------------
 * | TupleCount(4) | Tuple_1 offset(4) | Tuple_1 size(4) | ... |
 * ---------------------------------------------------------------
 * 
 * I wonder do we awaring the serialization method in tuple, since we are storing the tuple size as tuple data
 */

/**
 * @brief 
 * TablePage is the page that stores the real data. And since it's inherited from
 * page, we can use TablePage directly by using reinterpret_cast.
 * TablePage provided the interface that will store tuple in page. So you can regard this
 * class as a set of manipulating functions that is related to tuple storage.
 * Again, the key idea here is to provide a set of functions that can help us to store tuple in page.
 */
class TablePage: public Page {
public:
    void Init(page_id_t page_id, uint32_t page_size, page_id_t prev_page_id);

    // interface is really strange
    // FIXME: more elegant interface

    page_id_t GetTablePageId() {
        return *reinterpret_cast<page_id_t *> (GetData());
    }

    page_id_t GetPrevPageId() {
        return *reinterpret_cast<page_id_t *> (GetData() + OFFSET_PREV_PAGE_ID);
    }

    page_id_t GetNextPageId() {
        return *reinterpret_cast<page_id_t *> (GetData() + OFFSET_NEXT_PAGE_ID);
    }

    void SetPrevPageId(page_id_t prev_page_id) {
        *reinterpret_cast<uint32_t *> (GetData() + OFFSET_PREV_PAGE_ID) = prev_page_id;
    }

    void SetNextPageId(page_id_t next_page_id) {
        *reinterpret_cast<uint32_t *> (GetData() + OFFSET_NEXT_PAGE_ID) = next_page_id;
    }

    // tuple related

    bool InsertTuple(const Tuple &tuple, RID *rid);

    bool MarkDelete(const RID &rid);

    bool UpdateTuple(const Tuple &new_tuple, Tuple *old_tuple, const RID &rid);

    // TODO: figure out should we add a batch cleaning method

    void ApplyDelete(const RID &rid);

    void RollbackDelete(const RID &rid);

    bool GetTuple(const RID &rid, Tuple *tuple);

    bool GetFirstTupleRid(RID *first_rid);

    bool GetNextTupleRid(const RID &cur_rid, RID *next_rid);

private:
    // constant defintions and helper functions
    static_assert(sizeof(page_id_t) == 4);

    static constexpr size_t SIZE_TABLE_PAGE_HEADER = sizeof(lsn_t) + 3 * sizeof(page_id_t) + 2 * sizeof(uint32_t);
    static constexpr size_t OFFSET_PREV_PAGE_ID = SIZE_PAGE_HEADER;
    static constexpr size_t OFFSET_NEXT_PAGE_ID = OFFSET_PREV_PAGE_ID + sizeof(page_id_t);
    static constexpr size_t OFFSET_FREE_SPACE_PTR = OFFSET_NEXT_PAGE_ID + sizeof(page_id_t);
    static constexpr size_t OFFSET_TUPLE_COUNT = OFFSET_FREE_SPACE_PTR + sizeof(uint32_t);
    static constexpr size_t SIZE_SLOT = sizeof(uint32_t) * 2;  // 4 byte size, 4 byte offset

    uint32_t GetFreeSpacePointer() {
        return *reinterpret_cast<uint32_t *> (GetData() + OFFSET_FREE_SPACE_PTR);
    }

    void SetFreeSpacePointer(uint32_t free_space_pointer) {
        *reinterpret_cast<uint32_t *> (GetData() + OFFSET_FREE_SPACE_PTR) = free_space_pointer;
    }

    uint32_t GetTupleCount() {
        return *reinterpret_cast<uint32_t *> (GetData() + OFFSET_TUPLE_COUNT);
    }

    void SetTupleCount(uint32_t tuple_count) {
        *reinterpret_cast<uint32_t *> (GetData() + OFFSET_TUPLE_COUNT) = tuple_count;
    }

    uint32_t GetFreeSpaceRemaining() {
        return GetFreeSpacePointer() - SIZE_TABLE_PAGE_HEADER - SIZE_SLOT * GetTupleCount();
    }

    uint32_t GetTupleOffset(uint32_t slot_id) {
        return *reinterpret_cast<uint32_t *> (GetData() + SIZE_TABLE_PAGE_HEADER + SIZE_SLOT * slot_id);
    }

    void SetTupleOffset(uint32_t slot_id, uint32_t offset) {
        *reinterpret_cast<uint32_t *> (GetData() + SIZE_TABLE_PAGE_HEADER + SIZE_SLOT * slot_id) = offset;
    }

    uint32_t GetTupleSize(uint32_t slot_id) {
        return *reinterpret_cast<uint32_t *> (GetData() + SIZE_TABLE_PAGE_HEADER + SIZE_SLOT * slot_id + sizeof(uint32_t));
    }

    void SetTupleSize(uint32_t slot_id, uint32_t size) {
        *reinterpret_cast<uint32_t *> (GetData() + SIZE_TABLE_PAGE_HEADER + SIZE_SLOT * slot_id + sizeof(uint32_t)) = size;
    }

    /**
     * @brief 
     * return whether tuple is deleted or empty
     * @param tuple_size 
     * @return true 
     * @return false 
     */
    bool IsDeleted(uint32_t tuple_size) {
        return static_cast<bool> (tuple_size & DELETE_MASK) || tuple_size == 0;
    }

    /**
     * @brief
     * set the deleted flag in tuple_size
     * @param tuple_size 
     * @return uint32_t 
     */
    uint32_t SetDeletedFlag(uint32_t tuple_size) {
        return static_cast<uint32_t> (tuple_size | DELETE_MASK);
    }

    /**
     * @brief 
     * unset the deleted flag in tuple_size
     * @param tuple_size 
     * @return uint32_t 
     */
    uint32_t UnsetDeletedFlag(uint32_t tuple_size) {
        return static_cast<uint32_t> (tuple_size & (~DELETE_MASK));
    }
    

};

}

#endif