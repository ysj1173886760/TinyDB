/**
 * @file table_iterator.h
 * @author sheep
 * @brief table iterator
 * @version 0.1
 * @date 2022-05-11
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TABLE_ITERATOR_H
#define TABLE_ITERATOR_H

#include "storage/page/table_page.h"
#include "buffer/buffer_pool_manager.h"

namespace TinyDB {

class TableHeap;

/**
 * @brief 
 * iterator of table. i chose a different implementation from bustub.
 * because i want the user to read the tuple AFTER it has acquired the lock on it
 * so we will only read the tuple when we deference the iterator.
 * A pitfalls is that we might read the tuple that has been deleted.
 * So user should check whether tuple is valid before using it
 */
class TableIterator {
public:
    TableIterator()
        : table_heap_(nullptr),
          rid_(RID()),
          tuple_(Tuple()) {}

    TableIterator(TableHeap *table_heap, RID rid)
        : table_heap_(table_heap),
          rid_(rid),
          tuple_(Tuple()) {}

    TableIterator(const TableIterator &other)
        : table_heap_(other.table_heap_),
          rid_(other.rid_),
          tuple_(other.tuple_) {}

    inline void Swap(TableIterator &iter) {
        std::swap(iter.rid_, rid_);
        std::swap(iter.table_heap_, table_heap_);
        std::swap(iter.tuple_, tuple_);
    }

    inline bool operator==(const TableIterator &iter) const {
        return rid_ == iter.rid_;
    }

    inline bool operator!=(const TableIterator &iter) const {
        return !((*this) == iter);
    }

    const Tuple &operator*();

    Tuple *operator->();

    TableIterator &operator++();

    TableIterator operator++(int);

    TableIterator &operator=(const TableIterator &other) {
        table_heap_ = other.table_heap_;
        rid_ = other.rid_;
        tuple_ = other.tuple_;
        return *this;
    }

    void GetTuple();

private:
    TableHeap *table_heap_;
    RID rid_;
    Tuple tuple_;
};

}

#endif