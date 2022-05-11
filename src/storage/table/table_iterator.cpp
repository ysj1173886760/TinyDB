/**
 * @file table_iterator.cpp
 * @author sheep
 * @brief implementation of table iterator
 * @version 0.1
 * @date 2022-05-11
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/table/table_iterator.h"
#include "storage/table/table_heap.h"
#include "common/exception.h"

namespace TinyDB {

void TableIterator::GetTuple() {
    // At the end of the day, we wil call deserialize in tuple
    // which will handle previous tuple buffer for us
    bool res = table_heap_->GetTuple(rid_, &tuple_);
    // log the event
    if (!res) {
        LOG_INFO("Reading Invalid tuple though table iterator RID: %s", rid_.ToString().c_str());
    }
}

const Tuple &TableIterator::operator*() {
    TINYDB_ASSERT(rid_.GetPageId() != INVALID_PAGE_ID, "Invalid Table Iterator");
    if (tuple_.GetRID() != rid_) {
        GetTuple();
    }
    // still we may return with invalid tuple
    return tuple_;
}

Tuple *TableIterator::operator->() {
    TINYDB_ASSERT(rid_.GetPageId() != INVALID_PAGE_ID, "Invalid Table Iterator");
    if (tuple_.GetRID() != rid_) {
        GetTuple();
    }
    return &tuple_;
}

TableIterator &TableIterator::operator++() {
    BufferPoolManager *bpm = table_heap_->buffer_pool_manager_;
    auto cur_page = reinterpret_cast<TablePage *>(bpm->FetchPage(rid_.GetPageId()));
    // we should find a good way to handle out of memory issue here
    TINYDB_ASSERT(cur_page != nullptr, "Running out of memory");
    cur_page->RLatch();

    // we will not hold two latches at the same time
    // so there won't be a deadlock issue

    RID next_tuple_rid;
    if (!cur_page->GetNextTupleRid(rid_, &next_tuple_rid)) {
        // if we at the end of this page, try to fetch next page
        while (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
            auto next_page = reinterpret_cast<TablePage *>(bpm->FetchPage(cur_page->GetNextPageId()));
            if (next_page == nullptr) {
                THROW_OUT_OF_MEMORY_EXCEPTION("TableIterator::operator++ out of memory");
            }
            cur_page->RUnlatch();
            bpm->UnpinPage(cur_page->GetPageId(), false);
            cur_page = next_page;
            cur_page->RLatch();
            if (cur_page->GetFirstTupleRid(&next_tuple_rid)) {
                break;
            }
            // otherwise, try to fetch next page again
        }
    }
    cur_page->RUnlatch();
    bpm->UnpinPage(cur_page->GetPageId(), false);

    rid_ = next_tuple_rid;
    return *this;
}

TableIterator TableIterator::operator++(int) {
    // copy the old value.
    // i.e. rid, tuple
    TableIterator copy(*this);
    this->operator++();
    return copy;
}

}