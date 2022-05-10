/**
 * @file table_heap.h
 * @author sheep
 * @brief table heap
 * @version 0.1
 * @date 2022-05-10
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TABLE_HEAP_H
#define TABLE_HEAP_H

#include "buffer/buffer_pool_manager.h"
#include "storage/page/table_page.h"

namespace TinyDB {

/**
 * @brief 
 * TableHeap is a doubly-linked list of TablePages. It's the abstraction of heap file 
 * that provide high-level operations with tuple data. e.g. insert tuple without knowing real page
 */
class TableHeap {
public:
    ~TableHeap() = default;

    /**
     * @brief 
     * open an existing table heap
     * @param first_page_id id of the first page
     * @param buffer_pool_manager 
     */
    TableHeap(page_id_t first_page_id, BufferPoolManager *buffer_pool_manager):
        buffer_pool_manager_(buffer_pool_manager), first_page_id_(first_page_id) {
        TINYDB_ASSERT(first_page_id_ != INVALID_PAGE_ID, "Existing table heap should have at least one page");
    }

    bool InsertTuple(const Tuple &tuple, RID *rid);

    bool MarkDelete(const RID &rid);

    bool UpdateTuple(const Tuple &tuple, const RID &rid);

    void ApplyDelete(const RID &rid);

    void RollbackDelete(const RID &rid);

    bool GetTuple(const RID &rid, Tuple *tuple);

    inline page_id_t GetFirstPageId() const {
        return first_page_id_;
    }

private:
    BufferPoolManager *buffer_pool_manager_;
    page_id_t first_page_id_{INVALID_PAGE_ID};
};

}

#endif