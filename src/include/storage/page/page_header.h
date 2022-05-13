/**
 * @file page_header.h
 * @author sheep
 * @brief page header
 * @version 0.1
 * @date 2022-05-13
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef PAGE_HEADER_H
#define PAGE_HEADER_H

#include "common/config.h"

namespace TinyDB {

class PageHeader {
public:
    inline page_id_t GetPageId() {
        return page_id_;
    }

    inline void SetPageId(page_id_t page_id) {
        page_id_ = page_id;
    }

    inline lsn_t GetLSN() {
        return lsn_;
    }

    inline void SetLSN(lsn_t lsn) {
        lsn_ = lsn;
    }

protected:
    static_assert(sizeof(page_id_t) == 4);
    static_assert(sizeof(lsn_t) == 4);

    // 4 byte for page id
    // 4 byte for lsn
    static constexpr size_t SIZE_PAGE_HEADER = 8;
    static constexpr size_t OFFSET_LSN = 4;
    static constexpr size_t OFFSET_PAGE = 0;

    page_id_t page_id_;
    lsn_t lsn_;
};

}

#endif