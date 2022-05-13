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
#include "storage/page/page_header.h"

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
class TablePage: public PageHeader {
public:
    // TODO: figure out whether page_id and page_size is necessary
    /**
     * @brief 
     * initialize the TablePage header
     * @param page_id table page id
     * @param page_size size of this page
     * @param prev_page_id previous page id
     */
    void Init(page_id_t page_id, uint32_t page_size, page_id_t prev_page_id);

    // interface is really strange
    // FIXME: more elegant interface

    inline page_id_t GetPrevPageId() {
        return prev_page_id_;
    }

    inline page_id_t GetNextPageId() {
        return next_page_id_;
    }

    inline void SetPrevPageId(page_id_t prev_page_id) {
        prev_page_id_ = prev_page_id;
    }

    inline void SetNextPageId(page_id_t next_page_id) {
        next_page_id_ = next_page_id;
    }

    // tuple related

    /**
     * @brief 
     * insert a tuple into current page
     * @param tuple tuple to inserted
     * @param rid RID indicating tuple RID
     * @return true when insertion is succeed. i.e. there is enough space
     */
    bool InsertTuple(const Tuple &tuple, RID *rid);

    /**
     * @brief 
     * mark the tuple as deleted. the real deletion will performed by ApplyDelete at commit time
     * @param rid rid to the tuple to mark as deleted
     * @return true when deletion is succeed. i.e. tuple exists
     */
    bool MarkDelete(const RID &rid);

    /**
     * @brief 
     * update the tuple. Updation will fail when performing operation on deleted tuple(real deleted or mark deleted)
     * @param new_tuple new tuple value
     * @param old_tuple old tuple value, we will handle previous data in old_tuple
     * @param rid tuple rid
     * @return true when updation is succeed. i.e. tuple exists and we have enough space
     * to perform updation
     */
    bool UpdateTuple(const Tuple &new_tuple, Tuple *old_tuple, const RID &rid);

    // TODO: figure out should we add a batch cleaning method
    // for lock-based CC protocol, we might need to perform operation directly on one copy. So mark-apply deletion
    // will reduce the memory manipulation.
    // for MVCC, we only need insertion and deletion. which will convert to
    // insert new tuple, and modifying the metadata(timestamp). 
    // And background thread will need ApplyDelete to perform vacumming
    // TODO: figure out where should we store CC-related metadata

    /**
     * @brief 
     * delete tuple corresponding to rid. this will perform real deletion
     * @param rid 
     */
    void ApplyDelete(const RID &rid);

    /**
     * @brief 
     * to be called on abort. Rollback a delete. this will reverses a MarkDelete
     * @param rid 
     */
    void RollbackDelete(const RID &rid);

    /**
     * @brief get the tuple. GetTuple won't return a deleted tuple(real deleted or mark deleted)
     * @param rid rid of tuple
     * @param tuple tuple slot, we will handle previous data in tuple
     * @return true whether read is succeed
     */
    bool GetTuple(const RID &rid, Tuple *tuple);

    /**
     * @brief 
     * Get the first rid from current page. regard less whether tuple is mark deleted
     * @param first_rid 
     * @return true when we have tuple
     */
    bool GetFirstTupleRid(RID *first_rid);

    /**
     * @brief
     * Get the rid next to cur_rid. 
     * @param cur_rid 
     * @param next_rid 
     * @return true when we have next tuple
     */
    bool GetNextTupleRid(const RID &cur_rid, RID *next_rid);

    // constant defintions and helper functions
    static_assert(sizeof(page_id_t) == 4);

    static constexpr size_t SIZE_TABLE_PAGE_HEADER = sizeof(lsn_t) + 3 * sizeof(page_id_t) + 2 * sizeof(uint32_t);
    static constexpr size_t OFFSET_PREV_PAGE_ID = SIZE_PAGE_HEADER;
    static constexpr size_t OFFSET_NEXT_PAGE_ID = OFFSET_PREV_PAGE_ID + sizeof(page_id_t);
    static constexpr size_t OFFSET_FREE_SPACE_PTR = OFFSET_NEXT_PAGE_ID + sizeof(page_id_t);
    static constexpr size_t OFFSET_TUPLE_COUNT = OFFSET_FREE_SPACE_PTR + sizeof(uint32_t);

    // tuple slot format:
    // -------------------------------------------------------
    // | Tuple1_OFF | Tuple1_Size | Tuple2_OFF | Tuple2_Size |
    // -------------------------------------------------------
    static constexpr size_t OFFSET_SIZE = sizeof(uint32_t); // slot-level offset
    static constexpr size_t OFFSET_OFF = 0; // slot-level offset
    static constexpr size_t SIZE_SLOT = sizeof(uint32_t) * 2;  // 4 byte size, 4 byte offset

private:
    /**
     * @brief
     * get the raw pointer pointing to this page.
     * @return char* 
     */

    inline char *GetRawPointer() {
        // some evil experiments
        assert(reinterpret_cast<TablePage *>(data_ - SIZE_TABLE_PAGE_HEADER) == this);
        return data_ - SIZE_TABLE_PAGE_HEADER;
    }

    inline uint32_t GetFreeSpacePointer() {
        return free_space_pointer_;
    }

    inline void SetFreeSpacePointer(uint32_t free_space_pointer) {
        free_space_pointer_ = free_space_pointer;
    }

    inline uint32_t GetTupleCount() {
        return tuple_count_;
    }

    inline void SetTupleCount(uint32_t tuple_count) {
        tuple_count_ = tuple_count;
    }

    uint32_t GetFreeSpaceRemaining() {
        return GetFreeSpacePointer() - SIZE_TABLE_PAGE_HEADER - SIZE_SLOT * GetTupleCount();
    }

    uint32_t GetTupleOffset(uint32_t slot_id) {
        return *reinterpret_cast<uint32_t *> (data_ + SIZE_SLOT * slot_id + OFFSET_OFF);
    }

    void SetTupleOffset(uint32_t slot_id, uint32_t offset) {
        *reinterpret_cast<uint32_t *> (data_ + SIZE_SLOT * slot_id + OFFSET_OFF) = offset;
    }

    uint32_t GetTupleSize(uint32_t slot_id) {
        return *reinterpret_cast<uint32_t *> (data_ + SIZE_SLOT * slot_id + OFFSET_SIZE);
    }

    void SetTupleSize(uint32_t slot_id, uint32_t size) {
        *reinterpret_cast<uint32_t *> (data_ + SIZE_SLOT * slot_id + OFFSET_SIZE) = size;
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
    
    /**
     * @brief 
     * return whether tuple is valid
     * @param tuple_size 
     * @return true 
     * @return false 
     */
    bool IsValid(uint32_t tuple_size) {
        return tuple_size != 0;
    }

    // member variables
    page_id_t prev_page_id_;
    page_id_t next_page_id_;
    uint32_t free_space_pointer_;
    uint32_t tuple_count_;
    char data_[0];
};

}

#endif