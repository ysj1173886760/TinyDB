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

namespace TinyDB {

class IndexIterator {

};

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

}

#endif