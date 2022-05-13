/**
 * @file b_plus_tree_internal_page.h
 * @author sheep
 * @brief Internal node page of B+Tree
 * @version 0.1
 * @date 2022-05-13
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef B_PLUS_TREE_INTERNAL_PAGE_H
#define B_PLUS_TREE_INTERNAL_PAGE_H

#include "storage/page/b_plus_tree_page.h"

namespace TinyDB {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE \
    BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
public:
    /**
     * @brief 
     * initialize internal page of b+tree
     * @param page_id id of page
     * @param parent_id id of parent page
     * @param max_size max count of pairs that can stored in this page
     */
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, uint32_t max_size = INTERNAL_PAGE_SIZE);

    KeyType KeyAt(uint index) const;
    void SetKeyAt(uint index, const KeyType &key);
    int ValueIndex(const ValueType &value) const;
    ValueType ValueAt(uint index) const;
    ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const;
    void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);
    uint InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);
    void Remove(uint index);
    ValueType RemoveAndReturnOnlyChild();

    // split and merge utility methods

    void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm);
    void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);
    void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm);
    void MovelastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm);

private:
    void CopyNFrom(MappingType *items, uint size, BufferPoolManager *bpm);
    void CopyLastFrom(const MappingType &pair, BufferPoolManager *bpm);
    void CopyFirstFrom(const MappingType &pair, BufferPoolManager *bpm);

    static constexpr uint32_t INTERNAL_PAGE_SIZE = (PAGE_SIZE - BPLUSTREE_HEADER_SIZE) / sizeof(MappingType);

    // elastic array
    MappingType array_[0];
};

}

#endif