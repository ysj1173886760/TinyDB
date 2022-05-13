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
class BPlusTreePage {
public:
    bool IsLeafPage() const;
    bool IsRootPage() const;
    void SetPageType(IndexPageType page_type);

    int GetSize() const;
    void SetSize(int size);

    int GetMaxSize() const;
    void SetMaxSize() const;
    int GetMinSize() const;

    page_id_t GetParentPageId() const;
    void SetParentPageId() const;

    page_id_t GetPageId() const;
    void SetPageId(page_id_t page_id);

private:
    static_assert(sizeof(IndexPageType) == 4);
    // member varibles that both internal page and leaf page
    // will share
    page_id_t page_id_;
    lsn_t lsn_;
    uint32_t size_;
    uint32_t max_size_;
    page_id_t parent_page_id_;
    IndexPageType page_type_;
};

}

#endif