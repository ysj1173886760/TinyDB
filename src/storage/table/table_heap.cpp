/**
 * @file table_heap.cpp
 * @author sheep
 * @brief implementation of table heap
 * @version 0.1
 * @date 2022-05-10
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/table/table_heap.h"
#include "storage/page/table_page.h"
#include "common/exception.h"
#include "storage/table/table_iterator.h"

namespace TinyDB {

bool TableHeap::InsertTuple(const Tuple &tuple, RID *rid) {
    // we couldn't store it anyway
    if (tuple.GetSize() + TablePage::SIZE_TABLE_PAGE_HEADER + TablePage::SIZE_SLOT > PAGE_SIZE) {
        THROW_NOT_IMPLEMENTED_EXCEPTION("TinyDB Couldn't support very large tuple");
    }

    auto cur_page = static_cast<TablePage *> (buffer_pool_manager_->FetchPage(first_page_id_));
    if (cur_page == nullptr) {
        // we run out of memory, return false directly
        return false;
    }

    cur_page->WLatch();
    
    while (!cur_page->InsertTuple(tuple, rid)) {
        auto next_page_id = cur_page->GetNextPageId();
        
        // if next page is a valid page
        if (next_page_id != INVALID_PAGE_ID) {
            cur_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
            cur_page = static_cast<TablePage *> (buffer_pool_manager_->FetchPage(next_page_id));
            // TINYDB_ASSERT(cur_page != nullptr, "TinyDB running out of memory");
            // should we throw exception or return false?
            if (cur_page == nullptr) {
                // we are not holding any lock
                return false;
            }
            cur_page->WLatch();
        } else {
            // otherwise, we need to create a new page
            auto new_page = static_cast<TablePage *> (buffer_pool_manager_->NewPage(&next_page_id));
            if (new_page == nullptr) {
                cur_page->WUnlatch();
                buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
                return false;
            }
            // initialize the new page
            new_page->WLatch();
            cur_page->SetNextPageId(new_page->GetPageId());
            new_page->Init(new_page->GetPageId(), PAGE_SIZE, cur_page->GetPageId());
            // release the previous page
            cur_page->WUnlatch();
            // since we've modified the next page id, we need to flush it back to disk
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
            cur_page = new_page;
        }
    }

    // insertion is done
    // time to release cur_page
    cur_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);

    return true;
}

bool TableHeap::MarkDelete(const RID &rid) {
    auto page = reinterpret_cast<TablePage *> (buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return false;
    }

    page->WLatch();
    bool res = page->MarkDelete(rid);
    page->WUnlatch();
    // if we failed to mark delete, then we don't need to flush the page
    buffer_pool_manager_->UnpinPage(page->GetPageId(), res);

    return res;
}

bool TableHeap::UpdateTuple(const Tuple &tuple, const RID &rid) {
    auto page = reinterpret_cast<TablePage *> (buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return false;
    }

    // save the old value for rollback
    // only used in single-version CC protocol
    Tuple old_tuple;
    page->WLatch();
    bool res = page->UpdateTuple(tuple, &old_tuple, rid);
    page->WUnlatch();
    // same as MarkDelete
    // if we failed to update tuple, then we don't need to flush the page
    buffer_pool_manager_->UnpinPage(page->GetPageId(), res);

    return res;
}

void TableHeap::ApplyDelete(const RID &rid) {
    auto page = reinterpret_cast<TablePage *> (buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return;
    }
    page->WLatch();
    page->ApplyDelete(rid);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

// TODO: api design is really bad
// refactor is needed
void TableHeap::RollbackDelete(const RID &rid) {
    auto page = reinterpret_cast<TablePage *> (buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return;
    }
    page->WLatch();
    page->RollbackDelete(rid);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

bool TableHeap::GetTuple(const RID &rid, Tuple *tuple) {
    auto page = reinterpret_cast<TablePage *> (buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return false;
    }
    page->RLatch();
    bool res = page->GetTuple(rid, tuple);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return res;
}

TableIterator TableHeap::Begin() {
    TINYDB_ASSERT(first_page_id_ != INVALID_PAGE_ID, "invalid table heap");
    RID rid;
    auto cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    // shall we throw exception?
    TINYDB_ASSERT(cur_page != nullptr, "Running out of memory");
    // same logic as operator++ for table iterator
    cur_page->RLatch();

    if (!cur_page->GetFirstTupleRid(&rid)) {
        while (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
            auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page->GetNextPageId()));
            if (next_page == nullptr) {
                THROW_OUT_OF_MEMORY_EXCEPTION("TableHeap::Begin out of memory");
            }
            cur_page->RUnlatch();
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
            cur_page = next_page;
            cur_page->RLatch();
            if (cur_page->GetFirstTupleRid(&rid)) {
                break;
            }
            // otherwise, try the next page
        }
    }

    cur_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    return TableIterator(this, rid);
}

TableIterator TableHeap::End() {
    return TableIterator(this, RID(INVALID_PAGE_ID, 0));
}

}