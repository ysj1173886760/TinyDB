/**
 * @file index_iterator.h
 * @author sheep
 * @brief index iterator
 * @version 0.1
 * @date 2022-05-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef INDEX_ITERATOR_H
#define INDEX_ITERATOR_H

#include "common/rid.h"

#include <memory>

namespace TinyDB {

/**
 * @brief 
 * Base class for internal iterator. Other data-structure-specific iterator should
 * inherit this class, and implement the corresponding methods.
 * We will use IndexIterator(the one to expose to user) wrap internal iterator
 * for memory management. Think this just like RAII
 */
class InternalIterator {
public:
    InternalIterator() = default;
    virtual ~InternalIterator() {}

    /**
     * @brief 
     * Advance the iterator by one step
     */
    virtual void Advance() = 0;

    /**
     * @brief 
     * Get the RID
     * @return RID 
     */
    virtual RID Get() = 0;

    /**
     * @brief 
     * return whether iterator reaches the end
     */
    virtual bool IsEnd() = 0;
};

/**
 * @brief 
 * Wrapper class for internal iterator
 */
class IndexIterator {
public:
    explicit IndexIterator(std::unique_ptr<InternalIterator> it)
        : internal_iterator_(std::move(it)) {}
    
    /**
     * @brief 
     * Advance the iterator by one step
     */
    inline void Advance() {
        internal_iterator_->Advance();
    }

    /**
     * @brief 
     * Get the RID
     * @return RID 
     */
    inline RID Get() {
        return internal_iterator_->Get();
    }

    /**
     * @brief 
     * return whether iterator reaches the end
     */
    inline bool IsEnd() {
        return internal_iterator_->IsEnd();
    }

private:
    std::unique_ptr<InternalIterator> internal_iterator_;
};


}

#endif