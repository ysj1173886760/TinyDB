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

Result<> TableHeap::InsertTuple(const Tuple &tuple, RID *rid, const std::function<void(const RID &)> &callback) {
    // we couldn't store it anyway
    if (tuple.GetSize() + TablePage::SIZE_TABLE_PAGE_HEADER + TablePage::SIZE_SLOT > PAGE_SIZE) {
        THROW_NOT_IMPLEMENTED_EXCEPTION("TinyDB Couldn't support very large tuple");
    }

    auto cur_page = buffer_pool_manager_->FetchPage(first_page_id_);
    if (cur_page == nullptr) {
        // we run out of memory, return false directly
        return Result(ErrorCode::OUT_OF_MEMORY);
    }
    auto table_page = reinterpret_cast<TablePage *> (cur_page->GetData());

    cur_page->WLatch();
    
    while (!table_page->InsertTuple(tuple, rid)) {
        auto next_page_id = table_page->GetNextPageId();
        
        // if next page is a valid page
        if (next_page_id != INVALID_PAGE_ID) {
            cur_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
            cur_page = buffer_pool_manager_->FetchPage(next_page_id);
            if (cur_page == nullptr) {
                // we are not holding any lock
                return Result(ErrorCode::OUT_OF_MEMORY);
            }
            cur_page->WLatch();
        } else {
            // otherwise, we need to create a new page
            auto new_page = buffer_pool_manager_->NewPage(&next_page_id);
            if (new_page == nullptr) {
                cur_page->WUnlatch();
                buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
                return Result(ErrorCode::OUT_OF_MEMORY);
            }
            auto new_table_page = reinterpret_cast<TablePage *> (new_page->GetData());
            // initialize the new page
            new_page->WLatch();
            table_page->SetNextPageId(new_page->GetPageId());
            new_table_page->Init(new_page->GetPageId(), PAGE_SIZE, cur_page->GetPageId());
            // release the previous page
            cur_page->WUnlatch();
            // since we've modified the next page id, we need to flush it back to disk
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
            cur_page = new_page;
        }

        table_page = reinterpret_cast<TablePage *> (cur_page->GetData());
    }

    // callback, acquire the ownership of newly inserted tuple
    if (callback) {
        callback(*rid);
    }

    // insertion is done
    // time to release cur_page
    cur_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);

    return Result();
}

Result<> TableHeap::MarkDelete(const RID &rid) {
    auto page = (buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return Result(ErrorCode::OUT_OF_MEMORY);
    }
    auto table_page = reinterpret_cast<TablePage *> (page->GetData());

    page->WLatch();
    bool res = table_page->MarkDelete(rid);
    page->WUnlatch();
    // if we failed to mark delete, then we don't need to flush the page
    buffer_pool_manager_->UnpinPage(page->GetPageId(), res);

    if (res) {
        return Result();
    } else {
        // skip this tuple
        return Result(ErrorCode::SKIP);
    }
}

Result<> TableHeap::UpdateTuple(const Tuple &tuple, const RID &rid) {
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    if (page == nullptr) {
        return Result(ErrorCode::OUT_OF_MEMORY);
    }
    auto table_page = reinterpret_cast<TablePage *> (page->GetData());

    // save the old value for rollback
    // only used in single-version CC protocol
    Tuple old_tuple;
    page->WLatch();
    bool res = table_page->UpdateTuple(tuple, &old_tuple, rid);
    page->WUnlatch();
    // same as MarkDelete
    // if we failed to update tuple, then we don't need to flush the page
    buffer_pool_manager_->UnpinPage(page->GetPageId(), res);

    if (res) {
        return Result();
    } else {
        // we should abort this transaction
        return Result(ErrorCode::ABORT);
    }
}

void TableHeap::ApplyDelete(const RID &rid) {
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    if (page == nullptr) {
        return;
    }
    auto table_page = reinterpret_cast<TablePage *> (page->GetData());

    page->WLatch();
    table_page->ApplyDelete(rid);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

// TODO: api design is really bad
// refactor is needed
void TableHeap::RollbackDelete(const RID &rid) {
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    if (page == nullptr) {
        return;
    }
    auto table_page = reinterpret_cast<TablePage *> (page->GetData());

    page->WLatch();
    table_page->RollbackDelete(rid);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

Result<> TableHeap::GetTuple(const RID &rid, Tuple *tuple) {
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    if (page == nullptr) {
        return Result(ErrorCode::OUT_OF_MEMORY);
    }
    auto table_page = reinterpret_cast<TablePage *> (page->GetData());

    page->RLatch();
    bool res = table_page->GetTuple(rid, tuple);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    if (res) {
        return Result();
    } else {
        // skip this tuple
        return Result(ErrorCode::SKIP);
    }
}

TableIterator TableHeap::Begin() {
    TINYDB_ASSERT(first_page_id_ != INVALID_PAGE_ID, "invalid table heap");
    // default is invalid RID
    RID rid;
    auto cur_page = buffer_pool_manager_->FetchPage(first_page_id_);
    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(cur_page != nullptr, "");
    // same logic as operator++ for table iterator
    cur_page->RLatch();

    auto table_page = reinterpret_cast<TablePage *> (cur_page->GetData());

    if (!table_page->GetFirstTupleRid(&rid)) {
        while (table_page->GetNextPageId() != INVALID_PAGE_ID) {
            auto next_page = buffer_pool_manager_->FetchPage(table_page->GetNextPageId());
            TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(next_page != nullptr, "");

            cur_page->RUnlatch();
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
            cur_page = next_page;
            cur_page->RLatch();

            table_page = reinterpret_cast<TablePage *> (cur_page->GetData());
            if (table_page->GetFirstTupleRid(&rid)) {
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