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

#include "common/config.h"
#include "rwlatch.h"

namespace TinyDB {

class Page {
public:
    Page();
    ~Page() = default;

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