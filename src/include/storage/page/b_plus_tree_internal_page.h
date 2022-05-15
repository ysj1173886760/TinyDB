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

    /**
     * @brief 
     * get the key though the index
     * @param index 
     * @return KeyType 
     */
    KeyType KeyAt(int index) const;

    /**
     * @brief 
     * set the key though the index
     * @param index 
     * @param key 
     */
    void SetKeyAt(int index, const KeyType &key);

    /**
     * @brief 
     * find the index with the value equals to the input "value"
     * @param value 
     * @return index(>=0) corresponding to the value. or -1 when value doesn't exists
     */
    int ValueIndex(const ValueType &value) const;

    /**
     * @brief 
     * get the value though the index
     * @param index 
     * @return ValueType 
     */
    ValueType ValueAt(int index) const;

    /**
     * @brief 
     * find the child pointer which points to the page that contains input "key".
     * Start the search from the second key(the first key should always be invalid)
     * @param key 
     * @param comparator 
     * @return ValueType 
     */
    ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const;

    // insertion related

    /**
     * @brief 
     * populate new root page with old_value + new_key & new_value.
     * This method should be called whenever we created a new root page
     * @param old_value 
     * @param new_key 
     * @param new_value 
     */
    void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

    /**
     * @brief 
     * insert new_key & new_value pair right after the pair with it's value equals old_value
     * @param old_value 
     * @param new_key 
     * @param new_value 
     * @return uint32_t new size after insertion
     */
    uint32_t InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

    // split related

    /**
     * @brief 
     * remove half of key & value pairs from this page to recipient page
     * @param recipient 
     * @param bpm 
     */
    void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);

    // remove related

    /**
     * @brief 
     * remove the key & value pair in internal page corresponding to input index
     * @param index 
     */
    void Remove(int index);

    /**
     * @brief 
     * Remove the only key & value pair in the internal page and return the value
     * @return ValueType 
     */
    ValueType RemoveAndReturnOnlyChild();

    // merge related

    /**
     * @brief 
     * remove all of key & value pairs from current page to recipient. middle key is the separation key
     * that we should also added to the recipient to maintain the invariant. Bufferpool is also needed to
     * do the reparent
     * @param recipient 
     * @param middle_key 
     * @param bpm 
     */
    void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm);

    // redistributed related

    /**
     * @brief 
     * move the first key & value pair from current page to the tail of recipient page. We need bufferpool 
     * to do the reparent
     * @param recipient 
     * @param middle_key 
     * @param bpm 
     */
    void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm);

    /**
     * @brief 
     * remove the last key & value pair from this page to the head of recipient page.
     * @param recipient 
     * @param middle_key 
     * @param bpm 
     */
    void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm);

    static constexpr uint32_t INTERNAL_PAGE_SIZE = (PAGE_SIZE - BPLUSTREE_HEADER_SIZE) / sizeof(MappingType);

    void PrintAsBigint() {
        LOG_DEBUG("pageid: %d parent: %d size: %d", GetPageId(), GetParentPageId(), GetSize());
        for (int i = 0; i < GetSize(); i++) {
            LOG_DEBUG("%d %d", *reinterpret_cast<const int64_t *> (array_[i].first.ToBytes()), array_[i].second);
        }
    }

public:
    // helper functions

    /**
     * @brief 
     * Copy entries into current page, starting from "items" and copy "size" entries.
     * We also need to set the parent_page of these new items to current page since their parent has changed now.
     * @param items 
     * @param size 
     * @param bpm 
     */
    void CopyNFrom(MappingType *items, uint32_t size, BufferPoolManager *bpm);

    /**
     * @brief 
     * Append an entry at the end
     * @param pair 
     * @param bpm 
     */
    void CopyLastFrom(const MappingType &pair, BufferPoolManager *bpm);

    /**
     * @brief 
     * Append an entry at the begining.
     * @param pair 
     * @param bpm 
     */
    void CopyFirstFrom(const MappingType &pair, BufferPoolManager *bpm);

    // elastic array
    MappingType array_[0];
};

}

#endif