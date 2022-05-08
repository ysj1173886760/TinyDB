/**
 * @file page.h
 * @author sheep
 * @brief define basic meta-data within a page
 * i.e. page header
 * @version 0.1
 * @date 2022-04-30
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef PAGE_H
#define PAGE_H

#include <cstring>
#include <assert.h>

#include "common/config.h"
#include "common/rwlatch.h"

namespace TinyDB {

/**
 * @brief 
 * in memory representation of a single page,
 * we stored 8 byte metadata in the header,
 * first 4 byte is page id,
 * second 4 byte is lsn. i.e. last sequence number, used for crash recovery
 */
class Page {
    friend class BufferPoolManager;
public:
    Page() { ZeroData(); }
    ~Page() = default;

    /**
     * @brief get the data array stored in this page
     * @return actual data array
     */
    inline char *GetData() {
        return data_;
    }

    /**
     * @brief get id of this page
     * @return page_id_t 
     */
    inline page_id_t GetPageId() {
        return page_id_;
    }

    /**
     * @brief get ping count of this page
     * @return int
     */
    inline int GetPinCount() {
        return pin_count_;
    }

    /**
     * @brief return whether the page is dirty.
     * i.e. the content is different from disk
     * 
     * @return bool
     */
    inline bool IsDirty() {
        return is_dirty_;
    }

    inline void WLatch() {
        rwlatch_.WLock();
    }

    inline void WUnlatch() {
        rwlatch_.WUnlock();
    }

    inline void RLatch() {
        rwlatch_.RLock();
    }

    inline void RUnlatch() {
        rwlatch_.RUnlock();
    }

    // TODO: maintain recovery related information. i.e. lsn

protected:
    static_assert(sizeof(page_id_t) == 4);
    static_assert(sizeof(lsn_t) == 4);

    // 4 byte for page id
    // 4 byte for lsn
    static constexpr size_t PAGE_HEADER_SIZE = 8;
    static constexpr size_t OFFSET_LSN = 4;

private:
    // zero out the data
    inline void ZeroData() {
        memset(data_, 0, PAGE_SIZE);
    }

    // the actual data stored in a page
    // normally, we will reinterpret this data
    char data_[PAGE_SIZE]{};
    // the unique identifier of this page
    page_id_t page_id_{INVALID_PAGE_ID};
    // pin count of this page, used in buffer pool manager
    int pin_count_{0};
    // whether we have modified this page after
    // we bring it from disk to memory
    // if it's true, then we need to flush the data to disk
    // before eviciting this page
    bool is_dirty_{false};
    // page latch. used to protect the content
    ReaderWriterLatch rwlatch_;
};

}

#endif