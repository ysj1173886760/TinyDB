/**
 * @file table_page.h
 * @author sheep
 * @brief page that contains the real data
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

class TablePage: public Page {
public:
    void Init(page_id_t page_id, uint32_t page_size, page_id_t prev_page_id);

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
        memcpy(GetData() + OFFSET_PREV_PAGE_ID, &prev_page_id, sizeof(page_id_t));
    }

    void SetNextPageId(page_id_t next_page_id) {
        memcpy(GetData() + OFFSET_NEXT_PAGE_ID, &next_page_id, sizeof(page_id_t));
    }

private:
    // constant defintions and helper functions
    static_assert(sizeof(page_id_t) == 4);

    static constexpr size_t SIZE_TABLE_PAGE_HEADER = sizeof(lsn_t) + 3 * sizeof(page_id_t) + 2 * sizeof(uint32_t);
    static constexpr size_t OFFSET_PREV_PAGE_ID = PAGE_HEADER_SIZE;
    static constexpr size_t OFFSET_NEXT_PAGE_ID = OFFSET_PREV_PAGE_ID + sizeof(page_id_t);
    static constexpr size_t OFFSET_FREE_SPACE_PTR = OFFSET_NEXT_PAGE_ID + sizeof(page_id_t);
    static constexpr size_t OFFSET_TUPLE_COUNT = OFFSET_FREE_SPACE_PTR + sizeof(uint32_t);

};

}

#endif