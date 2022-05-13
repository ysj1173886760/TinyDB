/**
 * @file b_plus_tree_leaf_page.h
 * @author sheep
 * @brief leaf node page of B+Tree
 * @version 0.1
 * @date 2022-05-13
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef B_PLUS_TREE_LEAF_PAGE_H
#define B_PLUS_TREE_LEAF_PAGE_H

#include "storage/page/b_plus_tree_page.h"

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
public:
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, uint max_size = LEAF_PAGE_SIZE);

    page_id_t GetNextPageId() const;
    void SetNextPageId(page_id_t next_page_id);
    KeyType KeyAt(uint index) const;
    uint KeyIndex(const KeyType &key, const KeyComparator &comparator) const;
    const MappingType &GetItem(uint index);

    uint Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator);
    bool Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const;
    uint RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator);

    void MoveHalfTo(BPlusTreeLeafPage *recipient);
    void MoveAllTo(BPlusTreeLeafPage *recipient);
    void MoveFirstToEndOf(BPlusTreeLeafPage *recipient);
    void MoveLastToFrontOf(BPlusTreeLeafPage *recipient);

private:
    void CopyNFrom(MappingType *items, uint size);
    void CopyLastFrom(const MappingType &item);
    void CopyFirstFrom(const MappingType &item);

    static constexpr LEAF_PAGE_HEADER_SIZE = BPLUSTREE_HEADER_SIZE + sizeof(page_id_t);
    static constexpr LEAF_PAGE_SIZE = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType);

    // pointer pointing to next sibling page
    page_id_t next_page_id_;
    // elastic array. storing key-value pairs
    MappingType array_[0];
};

}

#endif