/**
 * @file b_plus_tree.h
 * @author sheep
 * @brief B+Tree
 * @version 0.1
 * @date 2022-05-13
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"

#include <string>
#include <unordered_set>
#include <deque>
#include <memory>

namespace TinyDB {

/**
 * @brief 
 * concurrent index execution context.
 * bustub was using transaction to recording these informations.
 * but i really want to make these modules isloated, so i decoupled ConcurrentIndexExecutionContext
 * from transaction
 */
struct BPlusTreeExecutionContext {
    // the pages that we latched during index operation
    std::unique_ptr<std::deque<Page *>> page_set_;
    // the page IDs that were deleted during index operation
    std::unique_ptr<std::unordered_set<page_id_t>> deleted_page_set_;

    BPlusTreeExecutionContext()
        : page_set_(new std::deque<Page *>()),
          deleted_page_set_(new std::unordered_set<page_id_t>()) {}

    inline void AddIntoPageSet(Page *page) {
        page_set_->push_back(page);
    }

    inline void AddIntoDeletedPageSet(page_id_t page_id) {
        deleted_page_set_->insert(page_id);
    }

    inline std::unordered_set<page_id_t> *GetDeletedPageSet() {
        return deleted_page_set_.get();
    }

    inline std::deque<Page *> *GetPageSet() {
        return page_set_.get();
    }

    inline void Reset() {
        page_set_->clear();
        deleted_page_set_->clear();
    }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
    using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
    using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

public:
    explicit BPlusTree(std::string index_name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                        uint32_t leaf_max_size = LeafPage::LEAF_PAGE_SIZE, uint32_t internal_max_size = InternalPage::INTERNAL_PAGE_SIZE);
    
    /**
     * @brief 
     * return whether b+tree is empty
     * @return
     */
    bool IsEmpty() const;

    /**
     * @brief 
     * Insert kv pair into B+tree
     * @param key 
     * @param value 
     * @param context 
     * @return true when insertion succeed, false when we are trying to insert duplicated key
     */
    bool Insert(const KeyType &key, const ValueType &value, BPlusTreeExecutionContext *context = nullptr);

    /**
     * @brief 
     * Delete kv pair associated with input key.
     * @param key 
     * @param context 
     * @return true when deletion succeed, false when we failed to find the key.
     */
    bool Remove(const KeyType &key, BPlusTreeExecutionContext *context = nullptr);

    /**
     * @brief
     * Return the value that associated with input key
     * @param key 
     * @param result 
     * @param context 
     * @return true when key exists
     */
    bool GetValue(const KeyType &key, std::vector<ValueType> *result, BPlusTreeExecutionContext *context = nullptr);

private:
    // helper functions

    /**
     * @brief 
     * Insert kv pair into an empty tree.
     * @param key 
     * @param value 
     */
    void StartNewTree(const KeyType &key, const ValueType &value);

    /**
     * @brief 
     * Insert constant kv pair into leaf page
     * @param key 
     * @param value 
     * @param context 
     * @return true when insertion succeed, false when we are trying to insert duplicated key
     */
    bool InsertIntoLeaf(const KeyType &key, const ValueType &value, BPlusTreeExecutionContext *context = nullptr);

    /**
     * @brief 
     * Insert kv pair into internal page after split
     * @param old_node 
     * @param key 
     * @param new_node 
     * @param context 
     */
    void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node, BPlusTreeExecutionContext *context = nullptr);

    /**
     * @brief 
     * Split input page and return newly created page.
     * @tparam N PageType, could be either internal page or leaf page
     * @param node 
     * @return N* new page
     */
    template <typename N>
    N *Split(N *node);

    /**
     * @brief 
     * Perform coalesce or redistribute based on sibling's size and node's size
     * @tparam N 
     * @param node 
     * @param context 
     * @return true when target leaf page should be deleted. otherwise no deletion happens
     */
    template <typename N>
    bool CoalesceOrRedistribute(N *node, BPlusTreeExecutionContext *context = nullptr);

    /**
     * @brief 
     * Move all the kv pairs from one page to its sibling page. And
     * parent page should be adjusted to take info of deletion into account.
     * @tparam N 
     * @param neighbor_node 
     * @param node 
     * @param parent 
     * @param index 
     * @param context 
     * @return true when parent node should be deleted. otherwise no deletion happens
     */
    template <typename N>
    bool Coalesce(N **neighbor_node, N **node, InternalPage **parent, int index, BPlusTreeExecutionContext *context = nullptr);

    /**
     * @brief 
     * Redistribute key & value pairs from one page to its sibling page.
     * if index == 0, then we move the the first item of neighbour_node to
     * the end of node. otherwise, we move the last item of neighbour_node to
     * the end of node.
     * @tparam N 
     * @param neighbor_node 
     * @param node 
     * @param index 
     */
    template <typename N>
    void Redistribute(N *neighbor_node, N *node, int index);

    /**
     * @brief 
     * Update root page if necessary
     * @param old_root_node 
     * @return true means root page should be deleted, false means no deletion happened
     */
    bool AdjustRoot(BPlusTreePage *old_root_node);

    /**
     * @brief 
     * Update the index metadata by flushing the root id into the disk.
     * I think we should hardcode the metadata page id, instead, we should
     * pass in some parameters to update the index metadata.
     * Another thing is that unlike the DDL, we can store metadata into table
     * to gain transactional catalog manipulation. We actually want the index metadata(root id)
     * to be changed and reflected at real time, so i think we need another type of page
     * to only store index metadata.
     * @param insert_record 
     */
    void UpdateRootPageId(bool insert_record = false);

    // member variables

    // index name
    std::string index_name_;
    // id of root page
    page_id_t root_page_id_;
    // buffer pool manager
    BufferPoolManager *buffer_pool_manager_;
    // comparator used to compare the key
    KeyComparator comparator_;
    // max size of leaf node
    uint32_t leaf_max_size_;
    // max size of internal node
    uint32_t internal_max_size_;
    // latch of root
    std::mutex root_latch_;
};

}

#endif