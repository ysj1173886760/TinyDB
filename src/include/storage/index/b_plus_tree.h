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
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
    using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
    using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

public:
    explicit BPlusTree(std::string index_name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                        uint32_t leaf_max_size = LeafPage::LEAF_PAGE_SIZE, uint32_t internal_max_size = InternalPage::INTERNAL_PAGE_SIZE);
    
    bool IsEmpty() const;

    bool Insert(const KeyType &key, const ValueType &value, BPlusTreeExecutionContext *context = nullptr);

    void Remove(const KeyType &key, BPlusTreeExecutionContext *context = nullptr);

    bool GetValue(const KeyType &key, std::vector<ValueType> *result, BPlusTreeExecutionContext *context = nullptr);

private:
    // helper functions

    void StartNewTree(const KeyType &key, const ValueType &value);

    bool InserIntoLeaf(const KeyType &key, const ValueType &value, BPlusTreeExecutionContext *context = nullptr);

    void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node, BPlusTreeExecutionContext *context = nullptr);

    template <typename N>
    N *Split(N *node);

    template <typename N>
    bool CoalesceOrRedistribute(N *node, BPlusTreeExecutionContext *context = nullptr);

    template <typename N>
    bool Coalesce(N **neighbor_node, N **node, InternalPage **parent, int index, BPlusTreeExecutionContext *context = nullptr);

    template <typename N>
    void Redistribute(N *neighbor_node, N *node, int index);

    bool AdjustRoot(BPlusTreePage *old_root_node);

    void UpdateRootPageId(int insert_record = 0);

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