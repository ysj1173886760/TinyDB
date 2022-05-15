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

#define B_PLUS_TREE_LEAF_PAGE_TYPE \
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
public:
    /**
     * @brief 
     * Initialize B+tree leaf page. we should call it whenever we create a new leaf node page
     * @param page_id 
     * @param parent_id 
     * @param max_size 
     */
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, uint32_t max_size = LEAF_PAGE_SIZE);

    /**
     * @brief
     * get the pointer pointing to next sibling page
     * @return page_id_t 
     */
    page_id_t GetNextPageId() const;

    /**
     * @brief
     * set the pointer pointing to next sibling page
     * @param next_page_id 
     */
    void SetNextPageId(page_id_t next_page_id);

    /**
     * @brief 
     * get key though index
     * @param index 
     * @return KeyType 
     */
    KeyType KeyAt(uint32_t index) const;

    /**
     * @brief 
     * find the first index i so that array[i].first >= key.
     * You can regard it as lower_bound in c++ std library.
     * @param key 
     * @param comparator 
     * @return index i
     */
    int KeyIndex(const KeyType &key, const KeyComparator &comparator) const;

    /**
     * @brief
     * get the key & value pair though "index"
     * @param index 
     * @return const MappingType& 
     */
    const MappingType &GetItem(uint32_t index);

    /**
     * @brief 
     * Insert key & value pair into leaf page ordered by key
     * @param key 
     * @param value 
     * @param comparator 
     * @return page size after insertion
     */
    uint32_t Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator);

    /**
     * @brief 
     * for the given key, check to see whether it exists in the leaf page and stores its
     * corresponding value in "value"
     * @param key 
     * @param value 
     * @param comparator 
     * @return whether key exists
     */
    bool Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const;

    /**
     * @brief 
     * find the key & value pair corresponding "key" and delete it.
     * @param key 
     * @param comparator 
     * @return whether deletion is performed
     */
    bool RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator);

    /**
     * @brief 
     * Remove ahlf of key & value pairs from current page to recipient page
     * @param recipient 
     */
    void MoveHalfTo(BPlusTreeLeafPage *recipient);

    /**
     * @brief 
     * Remove all of key & value pairs from current page to recipient page
     * @param recipient 
     */
    void MoveAllTo(BPlusTreeLeafPage *recipient);

    /**
     * @brief 
     * Remove the first key & value pair from current page to the end of recipient page
     * @param recipient 
     */
    void MoveFirstToEndOf(BPlusTreeLeafPage *recipient);

    /**
     * @brief 
     * Remove the last key & value pair from current page to the head of recipient page
     * @param recipient 
     */
    void MoveLastToFrontOf(BPlusTreeLeafPage *recipient);

    static constexpr uint32_t LEAF_PAGE_HEADER_SIZE = BPLUSTREE_HEADER_SIZE + sizeof(page_id_t);
    static constexpr uint32_t LEAF_PAGE_SIZE = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType);

    // for debug purpose
    void PrintAsBigint() {
        LOG_DEBUG("pageid: %d parent: %d size: %d", GetPageId(), GetParentPageId(), GetSize());
        for (int i = 0; i < GetSize(); i++) {
            LOG_DEBUG("%ld", *reinterpret_cast<const int64_t *> (array_[i].first.ToBytes()));
        }
    }

public:
    /**
     * @brief 
     * Copy "size" items starting from "items" to current page
     * @param items 
     * @param size 
     */
    void CopyNFrom(MappingType *items, int size);

    /**
     * @brief 
     * Copy the item into the end of current page
     * @param item 
     */
    void CopyLastFrom(const MappingType &item);

    /**
     * @brief 
     * Copy the item into the head of current page
     * @param item 
     */
    void CopyFirstFrom(const MappingType &item);

    // pointer pointing to next sibling page
    page_id_t next_page_id_;
    // elastic array. storing key-value pairs
    MappingType array_[0];
};

}

#endif