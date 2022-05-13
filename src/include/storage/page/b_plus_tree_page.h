/**
 * @file b_plus_tree_page.h
 * @author sheep
 * @brief base class for different type of B+Tree pages
 * @version 0.1
 * @date 2022-05-13
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef B_PLUS_TREE_PAGE_H
#define B_PLUS_TREE_PAGE_H

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"
#include "storage/page/page_header.h"

namespace TinyDB {

#define INDEX_TEMPLATE_ARGUMENTS \
    template <typename KeyType, typename ValueType, typename KeyComparator>

enum class IndexPageType {
    INVALID_INDEX_PAGE = 0,
    LEAF_PAGE,
    INTERNAL_PAGE,
};

/**
 * @brief 
 * Header part for B+Tree page. We inherit the basic header format from page.
 * We didn't inherit Page class directly since we want to manipulate the data just like
 * in memory data structure by reinterpreting data pointer to the BPlusTreePage.
 * Header format:
 * ----------------------------------------------------
 * | PageId(4) | LSN(4) | CurrentSize(4) | MaxSize(4) |
 * ----------------------------------------------------
 * | ParentPageId(4) | PageType(4) |
 * ---------------------------------
 */
class BPlusTreePage: public PageHeader {
public:
    /**
     * @brief 
     * return whether current page is leaf page
     * @return
     */
    inline bool IsLeafPage() const {
        return page_type_ == IndexPageType::LEAF_PAGE;
    }

    /**
     * @brief 
     * return whether current page is root page
     * @return true 
     * @return false 
     */
    bool IsRootPage() const {
        return parent_page_id_ == INVALID_PAGE_ID;
    }

    /**
     * @brief
     * set the type of current page
     * @param page_type 
     */
    void SetPageType(IndexPageType page_type) {
        page_type_ = page_type;
    }

    /**
     * @brief
     * get size of current page
     * @return int 
     */
    uint32_t GetSize() const {
        return size_;
    }

    /**
     * @brief
     * set size of current page
     * @param size 
     */
    void SetSize(uint32_t size) {
        size_ = size;
    }

    /**
     * @brief 
     * increase the size of current page by amount bytes
     * @param amount 
     */
    void IncreaseSize(int amount) {
        size_ += amount;
    }

    /**
     * @brief
     * get max page size.
     * when page size is greater than max size, we will trigger
     * a split on that page
     * @return int 
     */
    int GetMaxSize() const {
        return max_size_;
    }

    void SetMaxSize(uint32_t size) {
        max_size_ = size;
    }

    /**
     * @brief
     * get min page size. 
     * Generally, min page size == max page size / 2
     * @return uint32_t 
     */
    uint32_t GetMinSize() const {
        if (IsLeafPage()) {
            return max_size_ / 2;
        }
        return (max_size_ + 1) / 2;
    }

    page_id_t GetParentPageId() const {
        return parent_page_id_;
    }

    void SetParentPageId(page_id_t parent_page_id) {
        parent_page_id_ = parent_page_id;
    }

private:
    static_assert(sizeof(IndexPageType) == 4);
    // member varibles that both internal page and leaf page
    // will share
    uint32_t size_;
    uint32_t max_size_;
    page_id_t parent_page_id_;
    IndexPageType page_type_;
};

}

#endif