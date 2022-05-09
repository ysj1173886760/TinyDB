/**
 * @file table_page.cpp
 * @author sheep
 * @brief implementation of table page
 * @version 0.1
 * @date 2022-05-09
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/page/table_page.h"

// TODO: logic really need to be re-examined here

namespace TinyDB {

void TablePage::Init(page_id_t page_id, uint32_t page_size, page_id_t prev_page_id) {
    // TODO: figure out that do we really need "page_id"
    // since we are inheriting from page, which has already contains it's page_id
    *reinterpret_cast<page_id_t *> (GetData() + OFFSET_PAGE) = page_id;

    // we are double-linked list
    // and we are at the tail of the list
    SetPrevPageId(prev_page_id);
    SetNextPageId(INVALID_PAGE_ID);
    // pointing to the end of the page
    // TODO: does this varying?
    SetFreeSpacePointer(page_size);
    SetTupleCount(0);
}

bool TablePage::InsertTuple(const Tuple &tuple, RID *rid) {
    TINYDB_ASSERT(tuple.GetSize() > 0, "you shouldn't insert empty tuple");

    // check whether we can store this tuple
    if (GetFreeSpaceRemaining() < tuple.GetSize()) {
        return false;
    }

    // TODO: i wonder do we need to compact the slot
    // it looks like we won't delete the metadata(offset and size)

    // try to find a free slot to reuse
    // in case gcc will re-fetch over and over again
    uint32_t tuple_cnt = GetTupleCount();
    uint32_t slot_id = tuple_cnt;
    for (uint32_t i = 0; i < tuple_cnt; i++) {
        // check whether the slot is empty
        if (GetTupleSize(i) == 0) {
            slot_id = i;
            break;
        }
    }

    // If there is no more empty slot, then we need to recheck whether there is space left
    // for us to store tuple and one more slot
    if (slot_id == tuple_cnt && GetFreeSpaceRemaining() < tuple.GetSize() + SIZE_SLOT) {
        return false;
    }

    // update free space pointer
    SetFreeSpacePointer(GetFreeSpacePointer() - tuple.GetSize());
    // serialize it into the page
    tuple.SerializeTo(GetData() + GetFreeSpacePointer());

    // then update the slot pointer and size
    SetTupleOffset(slot_id, GetFreeSpacePointer());
    SetTupleSize(slot_id, tuple.GetSize());

    // set rid
    rid->Set(GetTablePageId(), slot_id);

    // if we are creating new slot, then we need to update tuple cnt
    if (slot_id == tuple_cnt) {
        SetTupleCount(tuple_cnt + 1);
    }

    return true;
}

bool TablePage::MarkDelete(const RID &rid) {
    TINYDB_ASSERT(rid.GetPageId() == GetPageId(), "Wrong page");
    uint32_t slot_id = rid.GetSlotId();
    // check whether slot id is vaild
    if (slot_id >= GetTupleCount()) {
        return false;
    }

    uint32_t tuple_size = GetTupleSize(slot_id);
    // check whether tuple is already deleted
    if (IsDeleted(tuple_size)) {
        return false;
    }

    // we don't want to delete a empty tuple
    if (tuple_size == 0) {
        return false;
    }
    SetTupleSize(slot_id, SetDeletedFlag(tuple_size));
    return true;
}

bool TablePage::UpdateTuple(const Tuple &new_tuple, Tuple *old_tuple, const RID &rid) {
    TINYDB_ASSERT(rid.GetPageId() == GetPageId(), "Wrong page");
    TINYDB_ASSERT(new_tuple.GetSize() > 0, "cannot insert empty tuples");
    uint32_t slot_id = rid.GetSlotId();
    // check the slot id
    if (slot_id >= GetTupleCount()) {
        return false;
    }

    uint32_t tuple_size = GetTupleSize(slot_id);
    // check whether tuple is deleted
    if (IsDeleted(tuple_size)) {
        return false;
    }

    // check whether we have enough space
    if (GetFreeSpaceRemaining() + tuple_size < new_tuple.GetSize()) {
        return false;
    }

    // copyout the old value
    // should we copyout the value only when pointer is not null?
    uint32_t tuple_offset = GetTupleOffset(slot_id);
    old_tuple->DeserializeFromInplace(GetData() + tuple_offset, tuple_size);
    old_tuple->SetRID(rid);

    uint32_t free_space_ptr = GetFreeSpacePointer();
    // move the data behind us(physically before us)
    // overlap awaring
    memmove(GetData() + free_space_ptr + tuple_size - new_tuple.GetSize(),
            GetData() + free_space_ptr,
            tuple_offset - free_space_ptr);
    // serialize new tuple
    new_tuple.SerializeTo(GetData() + tuple_offset + tuple_size - new_tuple.GetSize());
    SetTupleSize(slot_id, new_tuple.GetSize());
    SetFreeSpacePointer(free_space_ptr + tuple_size - new_tuple.GetSize());

    // we need to update offsets of tuple that is moved by us
    // note that offset has no correlation with slot id. so we need to 
    // check all of the slots
    uint32_t tuple_cnt = GetTupleCount();
    for (uint32_t i = 0; i < tuple_cnt; i++) {
        uint32_t tuple_offset_i = GetTupleOffset(i);
        // note that the reason we check offset_i < tuple_offset + tuple_size
        // instead of offset_i < tuple_offset is we need to also update the offset 
        // of new tuple.
        // another approach would be update new tuple offset individually. And condition here
        // should be offset_i < tuple_offset && i != slot_id
        if (GetTupleSize(i) != 0 && tuple_offset_i < tuple_offset + tuple_size) {
            SetTupleOffset(i, tuple_offset_i + tuple_size - new_tuple.GetSize());
        }
    }
    
    return true;
}

// perform the direct deletion.
void TablePage::ApplyDelete(const RID &rid) {
    TINYDB_ASSERT(rid.GetPageId() == GetPageId(), "Wrong page");
    uint32_t slot_id = rid.GetSlotId();
    TINYDB_ASSERT(slot_id < GetTupleCount(), "invalid slot id");

    uint32_t tuple_offset = GetTupleOffset(slot_id);
    uint32_t tuple_size = GetTupleSize(slot_id);

    if (IsDeleted(tuple_size)) {
        // mask out the delete bit
        tuple_size = UnsetDeletedFlag(tuple_size);
    }

    // move the data
    uint32_t free_space_ptr = GetFreeSpacePointer();
    memmove(GetData() + free_space_ptr + tuple_size,
            GetData() + free_space_ptr,
            tuple_offset - free_space_ptr);
    SetTupleSize(slot_id, 0);
    SetTupleOffset(slot_id, 0);
    SetFreeSpacePointer(free_space_ptr + tuple_size);
    
    // update tuple offsets
    auto tuple_cnt = GetTupleCount();
    for (uint32_t i = 0; i < tuple_cnt; i++) {
        auto tuple_offset_i = GetTupleOffset(i);
        if (GetTupleSize(i) != 0 && tuple_offset_i < tuple_offset) {
            SetTupleOffset(i, tuple_offset_i + tuple_size);
        }
    }
}

void TablePage::RollbackDelete(const RID &rid) {
    TINYDB_ASSERT(rid.GetPageId() == GetPageId(), "Wrong page");
    // just unset the delete flag
    uint32_t slot_id = rid.GetSlotId();
    TINYDB_ASSERT(slot_id < GetTupleCount(), "invalid slot id");
    uint32_t tuple_size = GetTupleSize(slot_id);

    if (IsDeleted(tuple_size)) {
        SetTupleSize(slot_id, UnsetDeletedFlag(tuple_size));
    }
}

bool TablePage::GetTuple(const RID &rid, Tuple *tuple) {
    TINYDB_ASSERT(rid.GetPageId() == GetPageId(), "Wrong page");
    uint32_t slot_id = rid.GetSlotId();
    // should we use assertion here?
    if (slot_id >= GetTupleCount()) {
        return false;
    }

    auto tuple_size = GetTupleSize(slot_id);
    // should we skip this tuple?
    // instead of aborting the txn
    // because in RC isolation level, it's very likely that we will read deleted tuple
    if (IsDeleted(tuple_size)) {
        return false;
    }

    auto tuple_offset = GetTupleOffset(slot_id);
    tuple->DeserializeFromInplace(GetData() + tuple_offset, tuple_size);
    tuple->SetRID(rid);

    return true;
}

// i think we should only skip those tuple that is really deleted instead of just a mark
// since txn may get aborted, and deletion may fail
bool TablePage::GetFirstTupleRid(RID *first_rid) {
    auto tuple_cnt = GetTupleCount();
    for (uint32_t i = 0; i < tuple_cnt; i++) {
        // find the first un-deleted tuple
        if (!IsDeleted(GetTupleSize(i))) {
            first_rid->Set(GetTablePageId(), i);
            return true;
        }
    }

    first_rid->Set(INVALID_PAGE_ID, 0);
    return false;
}

bool TablePage::GetNextTupleRid(const RID &cur_rid, RID *next_rid) {
    TINYDB_ASSERT(cur_rid.GetPageId() == GetPageId(), "Wrong page");
    // find the first valid tuple after cur_rid
    auto tuple_cnt = GetTupleCount();
    for (uint32_t i = cur_rid.GetSlotId() + 1; i < tuple_cnt; i++) {
        if (!IsDeleted(GetTupleSize(i))) {
            next_rid->Set(GetTablePageId(), i);
            return true;
        }
    }

    next_rid->Set(INVALID_PAGE_ID, 0);
    return false;
}

}