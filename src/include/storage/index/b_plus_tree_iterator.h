/**
 * @file b_plus_tree_iterator.h
 * @author sheep
 * @brief b+tree iterator
 * @version 0.1
 * @date 2022-05-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef B_PLUS_TREE_ITERATOR_H
#define B_PLUS_TREE_ITERATOR_H

#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/index/index_iterator.h"

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree;

#define BPLUSTREE_ITERATOR_TYPE BPlusTreeIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIterator : public InternalIterator {
public:
    BPlusTreeIterator(BufferPoolManager *bpm, Page *page, int index, BPlusTree<KeyType, ValueType, KeyComparator> *tree);
    BPlusTreeIterator() = default;

    void Advance() override;

    RID Get() override;

    bool IsEnd() override;

private:
    bool AdvanceHelper();

    static_assert(std::is_same<ValueType, RID>(), "we only support RID as ValueType");

    using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

    BufferPoolManager *buffer_pool_manager_{nullptr};
    // current page id
    page_id_t page_id_{INVALID_PAGE_ID};
    // current index
    int index_{0};
    // record the max key we read. To avoid dead lock
    KeyType key_;
    // current page
    LeafPage *leaf_page_{nullptr};
    // current page
    Page *page_;
    // store the b+tree to do the re-scan
    BPlusTree<KeyType, ValueType, KeyComparator> *tree_{nullptr};
};

}

#endif